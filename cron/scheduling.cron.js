const { pool } = require("../db");
const { enqueueBackupDBJob } = require("../queue/backup_db.queue");

async function runScheduledBackups() {
  const { rows } = await pool.query(`
    SELECT
      c.id AS connection_id,
      bs.default_backup_type,
      bs.storage_target,
      bs.timeout_minutes
    FROM connections c
    JOIN backup_settings bs
      ON bs.connection_id = c.id
    WHERE bs.scheduling_enabled = true
      AND c.status = 'VERIFIED';
  `);

  for (const row of rows) {
    const { rows: jobRows } = await pool.query(
      `
        INSERT INTO backup_jobs (
            connection_id,
            backup_type,
            trigger_type,
            status,
            created_at
        )
        SELECT
            $1,
            $2,
            'SCHEDULED',
            'QUEUED',
            now()
        WHERE NOT EXISTS (
            SELECT 1
            FROM backup_jobs bj
            WHERE bj.connection_id = $1
                AND bj.status IN ('QUEUED', 'RUNNING')
                AND bj.trigger_type = 'SCHEDULED'
            )
            RETURNING id;
      `,
      [row.connection_id, row.default_backup_type]
    );



    if (!jobRows.length) {
        throw new Error("Failed to create backup job");
    }

    await enqueueBackupDBJob({
      jobId: jobRows[0].id,
    });
  }
}

module.exports = { runScheduledBackups };
