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
    const STALE_JOB_MINUTES = 60;

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
        c.db_name,
        c.db_user_name,
        c.db_user_secret,

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
      db_name,
      db_user_name,
      db_user_secret,

      timeout_minutes,
      storage_target,
      s3_bucket,
      s3_region,
      local_storage_path,
      backup_upload_role_arn,
    } = jobData;

    //Decrypt DB password
    try {
      decryptedPassword = decrypt(db_user_secret);
    } catch {
      await failJob(jobId, "Failed to decrypt database credentials");
      return;
    }

    //Validate connection data
    if ( !db_host || !db_name || !db_user_name || !db_port ) {
      await failJob(jobId, "Invalid connection configuration");
      return;
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
      });
    } catch (err) {
      console.error("Unsupported backup type:", err);
      await failJob(jobId, "Unsupported database or backup type");
      return;
    }

    const resolvedPath = storage_target === "LOCAL" ? path.join( local_storage_path, `backup-${Date.now()}-${crypto.randomUUID()}.dump.gz`) : null;

    // Execute backup
    const storage = await createStorageStream(
      { storageTarget: storage_target, 
        resolvedPath, 
        s3Bucket: s3_bucket, 
        s3Region: s3_region, 
        backupUploadRoleARN: backup_upload_role_arn, 
      }
    );

    const runtimeTimeoutMinutes = Number.isFinite(timeout_minutes) && timeout_minutes > 0 ? timeout_minutes : 60;
    const runtimeTimeoutMs = runtimeTimeoutMinutes * 60 * 1000;

    const { bytesWritten, checksumSha256 } = await runBackup(command, storage, { timeoutMs: runtimeTimeoutMs, });

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
        storage.path,
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
