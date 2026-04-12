const { pool } = require("../db/index");
const { resolveCapabilitiesByEngine } = require ("../services/backupCapabilityService");
const { enqueueBackupDBJob } = require("../queue/backup_db.queue");
const { generatePresignedDownloadUrl } = require("../storage/presignDownload");

async function backupDB(req, res) {
  const { id: connectionId } = req.params;
  const { backupType, backupName } = req.body;

  if (!backupType) {
    return res.status(400).json({ error: "backupType is required" });
  }

  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    // Validate connection
    const { rows: connRows } = await client.query(
      `
      SELECT status
      FROM connections
      WHERE id = $1
      `,
      [connectionId]
    );

    if (!connRows.length) {
      return res.status(404).json({ error: "Connection not found" });
    }

    if (connRows[0].status !== "VERIFIED") {
      return res.status(400).json({ error: "Connection is not verified" });
    }

    const finalBackupName = backupName?.trim() || null;

    // Create backup job
    const { rows: jobRows } = await client.query(
      `
      INSERT INTO backup_jobs (connection_id, status, trigger_type, backup_name, backup_type)
      VALUES ($1, 'QUEUED', 'MANUAL', $2, $3)
      RETURNING id;
      `,
      [connectionId, finalBackupName, backupType]
    );

    const jobId = jobRows[0].id;

    // Enqueue worker
    try {
      await enqueueBackupDBJob({ jobId });
    } catch (err) {
      await client.query(
        `
        UPDATE backup_jobs
        SET status = 'FAILED',
            error = 'Failed to enqueue backup job',
            finished_at = now()
        WHERE id = $1
        `,
        [jobId]
      );

      await client.query("COMMIT");

      console.error("Enqueue backup job error:", err);
      return res.status(503).json({
        error: "Backup job could not be started. Please retry.",
      });
    }

    await client.query("COMMIT");

    return res.status(202).json({
      message: "Backup job started",
      jobId,
    });
  } catch (err) {
    await client.query("ROLLBACK");

    console.error("backupDB error:", err);
    return res.status(500).json({
      error: "Internal server error",
    });
  } finally {
    client.release();
  }
}


async function getBackupJobStatus(req, res) {
    try{
        const { jobId } = req.params;

       const { rows } = await pool.query(
            `
            SELECT
                bj.id,
                bj.connection_id,
                bj.status,
                bj.started_at,
                bj.finished_at,
                bj.error,
                bj.created_at,
                b.backup_type,
                b.backup_name,
                b.storage_target,
                b.backup_size_bytes
            FROM backup_jobs bj
            LEFT JOIN backups b
                ON b.id = bj.completed_backup_id
            WHERE bj.id = $1;
            `,
            [jobId]
        );

        if (!rows.length) {
            return res.status(404).json({ error: "Backup job not found" });
        }

        return res.json({
            jobId: rows[0].id,
            status: rows[0].status,
            backupType: rows[0].backup_type ?? null,
            backupName: rows[0].backup_name ?? null,
            storageTarget: rows[0].storage_target ?? null,
            sizeBytes: rows[0].backup_size_bytes ?? null,
            startedAt: rows[0].started_at,
            finishedAt: rows[0].finished_at,
            error: rows[0].error,
        });

    } catch (error) {
        console.error("Get backup job status error:", error);
        return res.status(500).json({
            error: "Internal server error",
        });
    }
  
}


async function getBackupCapabilities(req, res) {
    try {
        const dbId = req.params.id;
        //const userId = req.user.id;

        // Load database info
        const { rows }  = await pool.query(
        `
        SELECT id, db_type, status FROM connections WHERE id = $1
        `,
        [dbId]
        );

        if (!rows.length) {
            return res.status(404).json({ error: "Connection not found" });
        }

        const database = rows[0];

        // Status check
        if (database.status !== "VERIFIED") {
        return res.status(409).json({
            allowed: false,
            reason: `Database is in ${database.status} state`
        });
        }

        // Engine-based capabilities
        const capabilities = resolveCapabilitiesByEngine(database.db_type);

        return res.json({
        allowed: true,
        engine: database.db_type,
        ...capabilities,
        });

    } catch (error) {
        console.error("getBackupCapabilities error:", error);

        return res.status(500).json({
        allowed: false,
        reason: "Internal server error"
        });
    }
}


async function getBackups(req, res) {
    try {
        const { id } = req.params;

        const { rows } = await pool.query(
        `
            -- Completed backups
            SELECT
                b.id,
                b.backup_name,
                b.backup_type,
                b.backup_size_bytes,
                b.created_at,
                b.storage_target,
                b.storage_path,
                'COMPLETED'        AS status,
                NULL               AS error,
                NULL               AS started_at
            FROM backups b
            WHERE b.connection_id = $1

            UNION ALL

            -- Jobs (no artifact yet)
            SELECT
                bj.id,
                NULL,
                NULL,
                NULL,
                bj.created_at,
                NULL,
                NULL,
                bj.status,  
                bj.error,
                bj.started_at
            FROM backup_jobs bj
            WHERE bj.connection_id = $1
            AND bj.status IN ('QUEUED', 'RUNNING', 'FAILED')

            ORDER BY created_at DESC;
        `,
        [id]
        );

        return res.json({ data: rows });
    } catch (error) {
        console.error("Get backups error:", error);
        return res.status(500).json({ error: "Internal server error" });
    }
}


async function getUserBackups(req, res) {
    try {
        const { userId } = req.params;
        const { status = null, dbType = null, environment = null } = req.query;

        const parsedLimit = Number.parseInt(req.query.limit, 10);
        const parsedOffset = Number.parseInt(req.query.offset, 10);

        const limit = Number.isNaN(parsedLimit) ? 20 : parsedLimit;
        const offset = Number.isNaN(parsedOffset) ? 0 : parsedOffset;

        if (limit <= 0) {
            return res.status(400).json({ error: "limit must be greater than 0" });
        }

        if (offset < 0) {
            return res.status(400).json({ error: "offset must be >= 0" });
        }

        const normalizedStatus = status ? String(status).trim().toUpperCase() : null;
        const normalizedDbType = dbType ? String(dbType).trim() : null;
        const normalizedEnvironment = environment ? String(environment).trim() : null;

        const baseQuery = `
            WITH user_backups AS (
              -- COMPLETED backups
              SELECT
                b.id,
                b.connection_id,
                b.backup_name,
                b.backup_type,
                b.backup_size_bytes,
                b.created_at,
                b.storage_target,
                b.storage_path,
                'COMPLETED'::text AS status,
                NULL::text AS error,
                NULL::timestamptz AS started_at,
                c.db_name AS connection_name,
                c.db_type
              FROM backups b
              JOIN connections c ON c.id = b.connection_id
              JOIN user_connection_roles ucr ON ucr.connection_id = b.connection_id
              WHERE ucr.user_id = $1
                AND ($2::text IS NULL OR $2 = 'COMPLETED')

              UNION ALL

              -- JOBS (QUEUED, RUNNING, FAILED)
              SELECT
                bj.id,
                bj.connection_id,
                NULL::text,
                NULL::text,
                NULL::bigint,
                bj.created_at,
                NULL::text,
                NULL::text,
                bj.status,
                bj.error,
                bj.started_at,
                c.db_name AS connection_name,
                c.db_type
              FROM backup_jobs bj
              JOIN connections c ON c.id = bj.connection_id
              JOIN user_connection_roles ucr ON ucr.connection_id = bj.connection_id
              WHERE ucr.user_id = $1
                AND bj.status IN ('QUEUED', 'RUNNING', 'FAILED')
                AND ($2::text IS NULL OR bj.status = $2)
            )
        `;

        // COUNT query
        const countQuery = `
            ${baseQuery}
            SELECT COUNT(*)::int AS total
            FROM user_backups ub
            WHERE ($3::text IS NULL OR ub.db_type = $3);
        `;

        const countResult = await pool.query(countQuery, [
            userId,
            normalizedStatus,
            normalizedDbType,
            normalizedEnvironment
        ]);

        // DATA query
        const dataQuery = `
            ${baseQuery}
            SELECT
              ub.id,
              ub.connection_id,
              ub.backup_name,
              ub.backup_type,
              ub.backup_size_bytes,
              ub.created_at,
              ub.storage_target,
              ub.storage_path,
              ub.status,
              ub.error,
              ub.started_at,
              ub.connection_name,
              ub.db_type
            FROM user_backups ub
            WHERE ($3::text IS NULL OR ub.db_type = $3)
            ORDER BY ub.created_at DESC
            LIMIT $4 OFFSET $5;
        `;

        const { rows } = await pool.query(dataQuery, [
            userId,
            normalizedStatus,
            normalizedDbType,
            normalizedEnvironment,
            limit,
            offset
        ]);

        return res.json({
            data: rows,
            pagination: {
                total: countResult.rows[0].total,
                limit,
                offset
            }
        });

    } catch (error) {
        console.error("Get user backups error:", error);
        return res.status(500).json({ error: "Internal server error" });
    }
}


async function downloadBackup(req, res) {
    
    console.log("backup download route hit")
    const { backupId } = req.params;

    try{
        const { rows } = await pool.query(
            ` SELECT 
                bs.s3_bucket,
                bs.s3_region,
                bs.backup_restore_role_arn,
                b.storage_target,
                b.storage_path,
                b.checksum
            FROM backup_settings bs
            JOIN backups b
            ON b.connection_id=bs.connection_id
            WHERE b.id=$1
            `,[backupId]
        )

        if (rows.length === 0) {
            return res.status(404).json({ error: "Backup not found" });
        }

        const backup= rows[0]

        if (backup.storage_target !== "S3") {
            return res.status(400).json({ error: "Backup not stored in S3" });
        }

        if (!backup.backup_restore_role_arn || backup.backup_restore_role_arn.trim() === ""){
            return res.status(400).json({ error: "Backup Restore/ Download Arn not set" });
        }

        // S3-only
        const url = await generatePresignedDownloadUrl({
            bucket: backup.s3_bucket,
            region: backup.s3_region,
            path: backup.storage_path,
            roleArn: backup.backup_restore_role_arn
        });

        return res.json({
            downloadUrl: url,
            checksum: backup.checksum,
            checksumAlgo: "sha256"
        });

    } catch (error) {
        console.error("Backup download error:", error);
        return res.status(500).json({ error: "Internal server error", message: error.message });
    }
}


module.exports = { backupDB, getBackupJobStatus, getBackupCapabilities, getBackups, getUserBackups, downloadBackup };
