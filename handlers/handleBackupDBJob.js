require("dotenv").config();
const os = require("os");
const { pool } = require("../db/index");
const { decrypt } = require("../utils/crypto");
const { getBackupCommand } = require("../backup/strategy");
const { createStorageStream } = require("../storage/writer");
const { runBackup } = require("../backup/executor");
const { applyKeepLastNRetention } = require("../retention/keepLastN")
const path = require("path");

async function handleBackupDBJob(job) {
  const { jobId } = job.data;
  if (!jobId) return;

  console.log("Starting backup DB job:", jobId);

  const workerId = process.env.WORKER_ID || os.hostname();
  let decryptedPassword = null;

  try {
    const STALE_JOB_MINUTES = 5;

    //Claim the job
    const { rows } = await pool.query(
      `
      UPDATE backup_jobs bj
      SET status = 'RUNNING',
          started_at = now(),
          worker_id = $2,
          attempt = attempt + 1,
          error = NULL
      FROM connections c
      JOIN backup_settings bs
        ON bs.connection_id = c.id
      WHERE bj.id = $1
        AND bj.connection_id = c.id
        AND (
          bj.status = 'QUEUED'
          OR (
            bj.status = 'RUNNING'
            AND bj.started_at < now() - ($3 || ' minutes')::interval
          )
        )
      RETURNING
        bj.id,
        bj.backup_type,
        bj.backup_name,

        c.id AS connection_id,
        c.db_type,
        c.db_host,
        c.db_port,
        c.env_tag,
        c.db_name,
        c.db_user_name,
        c.db_user_secret,
        c.ssl_mode,

        bs.timeout_minutes,
        bs.storage_target,
        bs.s3_bucket,
        bs.s3_region,
        bs.local_storage_path,
        bs.backup_upload_role_arn,
        bs.retention_mode,
        bs.retention_value;
      `,
      [jobId, workerId, STALE_JOB_MINUTES]
    );

    if (!rows.length) {
      console.warn("Backup job skipped (already processed or locked):", jobId);
      return;
    }

    const jobData = rows[0];
    
    const {
      backup_type,
      backup_name,

      connection_id,
      db_type,
      db_host,
      db_port,
      env_tag,
      db_name,
      db_user_name,
      db_user_secret,
      ssl_mode,

      timeout_minutes,
      storage_target,
      s3_bucket,
      s3_region,
      local_storage_path,
      backup_upload_role_arn,
    } = jobData;

    //Decrypt DB password
    try {
      if (db_user_secret) {
        decryptedPassword = decrypt(db_user_secret);
      }
    } catch {
      await failJob(jobId, "Failed to decrypt database credentials");
      return;
    }

    //Validate connection data
    if (!db_type || !db_host || !db_name) {
      await failJob(jobId, "Invalid connection configuration");
      return;
    }

    // Port rules
    if ((db_type === "postgresql" || db_type === "mysql") && !db_port) {
      await failJob(jobId, "Port is required for this database engine");
      return;
    }

    // MongoDB: port optional (Atlas vs local)
    if ( db_type === "mongodb" &&  db_port !== null &&  db_port !== undefined && (typeof db_port !== "number" || db_port < 1 || db_port > 65535)) {
      await failJob(jobId, "Invalid MongoDB port");
      return;
    }

    // SSL validation (Postgres & MySQL only)
    if (db_type === "postgresql") {
      const valid = ["disable", "require", "verify-ca", "verify-full"];
      if (!ssl_mode || !valid.includes(ssl_mode)) {
        await failJob(jobId, "Invalid SSL mode configuration for PostgreSQL");
        return;
      }
    }

    if (db_type === "mysql") {
      const valid = ["disable", "require"];
      if (!ssl_mode || !valid.includes(ssl_mode)) {
        await failJob(jobId, "Invalid SSL mode configuration for MySQL");
        return;
      }
    }

    if (
      env_tag === "production" &&
      (db_type === "postgresql" || db_type === "mysql") &&
      ssl_mode === "disable"
    ) {
      await failJob(jobId, "SSL must be enabled for production databases");
      return;
    }

    //credentials
    //PostgreSQL-username & password required
    if (
      db_type === "postgresql" &&
      (!db_user_name || !decryptedPassword)
    ) {
      await failJob(jobId, "Username and password are required for PostgreSQL");
      return;
    }

    //MySQL-username required, password optional
    if (
      db_type === "mysql" &&
      !db_user_name
    ) {
      await failJob(jobId, "Username is required for MySQL");
      return;
    }

    //MongoDB-if one exists, both required
    if (db_type === "mongodb") {
      if (
        (db_user_name && !decryptedPassword) ||
        (!db_user_name && decryptedPassword)
      ) {
        await failJob(jobId, "Both username and password are required for MongoDB authentication");
        return;
      }
    }



    if (!backup_type || !storage_target) {
      await failJob(jobId, "Invalid backup job payload");
      return;
    }

    if ( storage_target === "LOCAL" && !local_storage_path ) {
      await failJob(jobId, "Missing local storage path");
      return;
    }

    if ( storage_target === "S3" && (!s3_bucket || !s3_region || !backup_upload_role_arn) ) {
      await failJob(jobId, "Missing S3 storage configuration");
      return;
    }

    //Build backup command
    let command;
    try {
      command = getBackupCommand(db_type, backup_type, {
        host: db_host,
        port: db_port,
        user: db_user_name,
        password: decryptedPassword,
        database: db_name,
        sslMode: ssl_mode,
      });
    } catch (err) {
      console.error("Unsupported backup type:", err);
      await failJob(jobId, "Unsupported database or backup type");
      return;
    }

    const storageConfig = {
      storageTarget: storage_target,
      localStoragePath: local_storage_path,
      s3Bucket: s3_bucket,
      s3Region: s3_region,
      backupUploadRoleARN: backup_upload_role_arn,
      alreadyCompressed: command.alreadyCompressed,
      extension: command.extension
    };

    const runtimeTimeoutMinutes = Number.isFinite(timeout_minutes) && timeout_minutes > 0 ? timeout_minutes : 60;
    const runtimeTimeoutMs = runtimeTimeoutMinutes * 60 * 1000;

    const { bytesWritten, checksumSha256, storagePath } = await runBackup(command, () => createStorageStream(storageConfig), { timeoutMs: runtimeTimeoutMs, });

    // Persist backup artifact
    const backupResult = await pool.query(
      `
      INSERT INTO backups (
        connection_id,
        backup_job_id,
        storage_target,
        storage_path,
        backup_type,
        backup_name,
        backup_size_bytes,
        checksum
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      RETURNING id;
      `,
      [
        connection_id,
        jobId,
        storage_target,
        storagePath,
        backup_type,
        backup_name || null,
        bytesWritten,
        checksumSha256
      ]
    );

    const backupId = backupResult.rows[0].id;

    // Mark job as completed
    await pool.query(
      `
      UPDATE backup_jobs
      SET status = 'COMPLETED',
          finished_at = now(),
          completed_backup_id = $2
      WHERE id = $1
        AND status = 'RUNNING';
      `,
      [jobId, backupId]
    );

    await applyKeepLastNRetention(jobData.connection_id);

    console.log("Backup completed:", backupId);
  } catch (err) {
    console.error("Backup execution error:", err);
    await failJob(jobId, getBackupExecutionErrorMessage(err));
  } finally {
    decryptedPassword = null; // security hygiene
  }
}

//Helper: fail job safely
async function failJob(jobId, message) {
  await pool.query(
    `
    UPDATE backup_jobs
    SET status = 'FAILED',
        finished_at = now(),
        error = $2
    WHERE id = $1
      AND status = 'RUNNING';
    `,
    [jobId, message]
  );
}

function getBackupExecutionErrorMessage(err) {
  if (!err) return "Unknown backup execution error";

  if (err.code === "ENOENT") {
    return "Backup tool not available on server";
  }

  if (err.code === "ETIMEDOUT") {
    return "Backup exceeded maximum runtime";
  }

  if (err.message?.toLowerCase().includes("authentication")) {
    return "Database authentication failed";
  }

  if (err.message?.toLowerCase().includes("permission denied")) {
    return "Insufficient permissions to perform backup";
  }

  if (err.message?.toLowerCase().includes("write")) {
    return "Failed to write backup to storage";
  }

  if (typeof err.exitCode === "number") {
    return `Backup process exited with code ${err.exitCode}`;
  }

  return "Unexpected error during backup execution";
}

module.exports = { handleBackupDBJob };
