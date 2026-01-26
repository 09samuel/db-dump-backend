const { pool } = require("../db/index");
const { resolveCapabilitiesByEngine } = require ("../services/backupCapabilityService");
const { enqueueBackupDBJob } = require("../queue/backup_db.queue");
const { resolveStorageConfig } = require("../utils/storageResolver")

// async function backupDB(req, res) {
    
//     const { backupType, storageTarget, backupName }= req.body;
//     const { id } = req.params;

//     if (!backupType) {
//         return res.status(400).json({ error: 'backupType is required' });
//     }

//     if (!storageTarget) {
//         return res.status(400).json({ error: 'storageTarget is required' });
//     }

//     const client = await pool.connect();

//     try {
//         const {rows} = await client.query(
//             `
//             SELECT id, status FROM connections WHERE id = $1
//             `,
//             [id]
//         );

//         if (!rows.length) {
//             return res.status(404).json({ error: "Connection not found" });
//         }

//         if (rows[0].status !== 'VERIFIED') {
//             return res.status(400).json({ error: "Connection is not verified" });
//         }

//         await client.query("BEGIN");

//         const jobResult = await client.query(
//              `
//             INSERT INTO backup_jobs (connection_id, status, trigger_type)
//             VALUES ($1, 'QUEUED', 'MANUAL')
//             RETURNING id;
//             `,
//             [id]
//         );
  
//         const jobId = jobResult.rows[0].id;

//         try {
//             await enqueueBackupDBJob({jobId, backupType, storageTarget, backupName});
//         } catch (enqueueError) {
//             await client.query(
//                 `
//                 UPDATE backup_jobs
//                 SET status = 'ERROR',
//                     error = 'Failed to enqueue backup job',
//                     finished_at = now()
//                 WHERE id = $1
//                 `,
//                 [jobId]
//             );

//             await client.query("COMMIT");

//             console.error("Enqueue backup job error:", enqueueError);
//             return res.status(503).json({
//                 error: "Backup job could not be started. Please retry.",
//             });
//         }


//         await client.query("COMMIT");

//         return res.json({
//             message: "Backup job started",
//             jobId: jobId,
//         });

//     } catch (error) {
//         await client.query("ROLLBACK");
//         console.error("Backup DB error:", error);
//         return res.status(500).json({
//             error: "Internal server error",
//         });
//     } finally {
//         client.release();
//     }
// }




async function backupDB(req, res) {
    const { id: connectionId } = req.params;
    const { backupName, backupType } = req.body;

    // if (!backupName) {
    //     return res.status(400).json({ error: 'backupName is required' });
    // }

    if (!backupType) {
        return res.status(400).json({ error: 'backupType is required' });
    }


    const client = await pool.connect();

    try {
        await client.query("BEGIN");

        //Validate connection
        const connResult = await client.query(
        `
        SELECT id, status
        FROM connections
        WHERE id = $1
        `,
        [connectionId]
        );

        if (!connResult.rows.length) {
            return res.status(404).json({ error: "Connection not found" });
        }

        if (connResult.rows[0].status !== "VERIFIED") {
            return res.status(400).json({ error: "Connection is not verified" });
        }

    
        //Load backup settings
        const settingsResult = await client.query(
        `
        SELECT *
        FROM backup_settings
        WHERE connection_id = $1
        `,
        [connectionId]
        );

        if (!settingsResult.rows.length) {
        return res.status(400).json({
            error: "Backup settings are not configured for this connection",
        });
        }

        const settings = settingsResult.rows[0];


        //Resolve final configuration
        const resolvedBackupType =  backupType ?? settings.default_backup_type;

        //LOCAL never enforces retention
        const retentionEnabled = settings.storage_target === "LOCAL"  ? false : settings.retention_enabled;

        const retentionMode = retentionEnabled ? settings.retention_mode : null;
        const retentionValue = retentionEnabled ? settings.retention_value : null;

        //console.log(settings)

        const storageConfig = resolveStorageConfig(settings);

        if (!storageConfig) {
            return res.status(400).json({
                error: "Unsupported storage target",
            });
        }


        //Create backup job 
        const jobResult = await client.query(
             `
            INSERT INTO backup_jobs (connection_id, status, trigger_type)
            VALUES ($1, 'QUEUED', 'MANUAL')
            RETURNING id;
            `,
            [connectionId]
        );

        const jobId = jobResult.rows[0].id;

        //Enqueue worker
        try {
            await enqueueBackupDBJob({
                jobId,
                backupType: resolvedBackupType,
                backupName,
                timeoutMinutes: settings.timeout_minutes,
                ...storageConfig,
            });

        } catch (enqueueError) {
            await client.query(
                `
                UPDATE backup_jobs
                SET status = 'ERROR',
                    error = 'Failed to enqueue backup job',
                    finished_at = now()
                WHERE id = $1
                `,
                [jobId]
            );

            await client.query("COMMIT");

            console.error("Enqueue backup job error:", enqueueError);
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



module.exports = { backupDB, getBackupJobStatus, getBackupCapabilities, getBackups };