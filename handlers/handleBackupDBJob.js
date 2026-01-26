require("dotenv").config();
const os = require("os");
const { pool } = require("../db/index");
const { decrypt } = require("../utils/crypto");
const { getBackupCommand } = require("../backup/strategy");
const { createStorageStream } = require("../storage/writer");
const { runBackup } = require("../backup/executor");

async function handleBackupDBJob(job) {
  const { jobId, backupType, backupName, storageTarget, resolvedPath, s3Bucket, s3Region, roleArn, timeoutMinutes} = job.data;
  if (!jobId) return;

  console.log("Starting backup DB job:", jobId);

  const workerId = process.env.WORKER_ID || os.hostname();
  let decryptedPassword = null;

  try {
    const MAX_BACKUP_RUNTIME_MINUTES = Number.isFinite(timeoutMinutes) && timeoutMinutes > 0 ? timeoutMinutes : 60;


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
      WHERE bj.id = $1
        AND (
          bj.status = 'QUEUED'
          OR (
            bj.status = 'RUNNING'
            AND bj.started_at < now() - ($3 || ' minutes')::interval
          )
        )
        AND bj.connection_id = c.id
      RETURNING
        bj.id,
        bj.connection_id,
        c.db_type,
        c.db_host,
        c.db_port,
        c.db_name,
        c.db_user_name,
        c.db_user_secret;
      `,
      [jobId, workerId, MAX_BACKUP_RUNTIME_MINUTES]
    );

    if (!rows.length) {
      console.warn("Backup job skipped (already processed or locked):", jobId);
      return;
    }

    const jobData = rows[0];

    //Decrypt DB password
    try {
      decryptedPassword = decrypt(jobData.db_user_secret);
    } catch {
      await failJob(jobId, "Failed to decrypt database credentials");
      return;
    }

    //Validate connection data
    // if ( !jobData.db_host || !jobData.db_name || !jobData.db_user_name || !jobData.db_port ) {
    //   await failJob(jobId, "Invalid connection configuration");
    //   return;
    // }

    // if (!backupType || !storageTarget) {
    //   await failJob(jobId, "Invalid backup job payload");
    //   return;
    // }

    // if ( storageTarget === "LOCAL" && !resolvedPath ) {
    //   await failJob(jobId, "Missing local storage path");
    //   return;
    // }

    // if ( storageTarget === "S3" && (!s3Bucket || !s3Region) ) {
    //   await failJob(jobId, "Missing S3 storage configuration");
    //   return;
    // }


    //Build backup command
    let command;
    try {
      command = getBackupCommand(jobData.db_type, backupType, {
        host: jobData.db_host,
        port: jobData.db_port,
        user: jobData.db_user_name,
        password: decryptedPassword,
        database: jobData.db_name,
      });
    } catch (err) {
      console.error("Unsupported backup type:", err);
      await failJob(jobId, "Unsupported database or backup type");
      return;
    }


    // Execute backup
    const storage = await createStorageStream({ storageTarget, resolvedPath, s3Bucket, s3Region, roleArn, });
    const bytesWritten = await runBackup(command, storage);


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
        backup_size_bytes
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7)
      RETURNING id;
      `,
      [
        jobData.connection_id,
        jobId,
        storageTarget,
        storage.path,
        backupType,
        backupName || null,
        bytesWritten,
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
