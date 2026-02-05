require("dotenv").config();
const { pool } = require("../db");
const { enqueueBackupDBJob } = require("../queue/backup_db.queue");
const { computeNextRunAt } = require("../utils/cronCompute")

async function runScheduledBackups() {
    console.log("[SCHEDULER] Scheduled backup run started");

    const { rows } = await pool.query(`
        SELECT
            c.id AS connection_id,
            bs.default_backup_type,
            bs.timeout_minutes,
            bs.cron_expression
        FROM connections c
        JOIN backup_settings bs
            ON bs.connection_id = c.id
        WHERE bs.scheduling_enabled = true
            AND bs.next_run_at <= now()
            AND c.status = 'VERIFIED';
    `);

    console.log(`[SCHEDULER] Found ${rows.length} eligible connections`);

    let enqueuedCount = 0;

    for (const row of rows) {
        try {
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

            // no insert = already scheduled → normal
            if (!jobRows.length) {
                continue;
            }

            await enqueueBackupDBJob({ jobId: jobRows[0].id });
            enqueuedCount++;

            const nextRunAt = computeNextRunAt(row.cron_expression)

            await pool.query(
                `
                UPDATE backup_settings
                SET next_run_at = $1
                WHERE connection_id = $2;
                `,
                [nextRunAt, row.connection_id]
            );
        } catch (err) {
            console.error(
                `[SCHEDULER] Failed to enqueue backup for connection ${row.connection_id}`,
                err
            );
        }
    }

    console.log(`[SCHEDULER] Enqueued ${enqueuedCount} backup jobs`);
}

module.exports = { runScheduledBackups };
