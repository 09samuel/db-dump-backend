const { pool } = require("../db/index");
const { resolveCapabilitiesByEngine } = require ("../services/backupCapabilityService");
const { enqueueBackupDBJob } = require("../queue/backup_db.queue");

async function backupDB(req, res) {
    
    const { backupType, storageTarget, backupName }= req.body;
    const { id } = req.params;

    if (!backupType) {
        return res.status(400).json({ error: 'backupType is required' });
    }

    if (!storageTarget) {
        return res.status(400).json({ error: 'storageTarget is required' });
    }


    const client = await pool.connect();

    try {
        const {rows} = await client.query(
            `
            SELECT id, status FROM connections WHERE id = $1
            `,
            [id]
        );

        if (!rows.length) {
            return res.status(404).json({ error: "Connection not found" });
        }

        if (rows[0].status !== 'VERIFIED') {
            return res.status(400).json({ error: "Connection is not verified" });
        }

        await client.query("BEGIN");

        const jobRows = await client.query(
            `
            INSERT INTO backup_jobs (connection_id, status, storage_target, backup_type, backup_name)
            VALUES ($1,'QUEUED', $2, $3, $4)
            RETURNING id
            
            `,
            [id, storageTarget, backupType, backupName?? null]
        );
        
        const jobId = jobRows.rows[0].id;

        try {
            await enqueueBackupDBJob(jobId);
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

        return res.json({
            message: "Backup job started",
            jobId: jobId,
        });

    } catch (error) {
        await client.query("ROLLBACK");
        console.error("Backup DB error:", error);
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
            id,
            connection_id,
            status,
            storage_target,
            backup_type,
            backup_name,
            started_at,
            finished_at,
            error,
            created_at
            FROM backup_jobs
            WHERE id = $1
            `,
            [jobId]
        );

        if (!rows.length) {
            return res.status(404).json({ error: "Backup job not found" });
        }

        return res.json({
            connectionId: rows[0].connectionId,
            status: rows[0].status,
            storageTarget: rows[0].storageTarget,
            backupType: rows[0].backup_type,
            backupName: rows[0].backup_name,
            startedAt: rows[0].startedAt,
            finishedAt: rows[0].finishedAt,
            error: rows[0].error,
            createdAt: rows[0].createdAt
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
      reason: null
    });
  } catch (error) {
    console.error("getBackupCapabilities error:", error);

    return res.status(500).json({
      allowed: false,
      reason: "Internal server error"
    });
  }
}




module.exports = { backupDB, getBackupJobStatus, getBackupCapabilities };