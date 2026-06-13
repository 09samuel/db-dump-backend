require("dotenv").config();
const { pool } = require("../db");
const { enqueueBackupDBJob } = require("../queue/backup_db.queue");
const { computeNextRunAt } = require("../utils/cronCompute")
const { insertAuditLog } = require("../utils/auditLogger");

async function runScheduledBackups() {
    console.log("[SCHEDULER] Scheduled backup run started");

    await insertAuditLog({
        roleAtTime: "SYSTEM",
        actionType: "SCHEDULED_BACKUP_SCAN",
        actionCategory: "SYSTEM",
        resourceType: "SCHEDULER",
        message: "Scheduled backup scan started",
        status: "SUCCESS",
    });

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
                    actor_role_at_time,
                    created_at,
                    owner_id
                )
                SELECT
                    $1,
                    $2,
                    'SCHEDULED',
                    'QUEUED',
                    'SYSTEM',
                    now(),
                    (SELECT user_id FROM user_connection_roles WHERE connection_id = $1 AND role = 'OWNER' LIMIT 1)
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

            await insertAuditLog({
                roleAtTime: "SYSTEM",
                actionType: "SCHEDULED_BACKUP_ENQUEUED",
                actionCategory: "BACKUP",
                resourceType: "BACKUP_JOB",
                resourceId: jobRows[0].id,
                message: "Scheduled backup job enqueued",
                status: "SUCCESS",
                metadata: { connectionId: row.connection_id, backupType: row.default_backup_type },
            });
        } catch (err) {
            console.error(
                `[SCHEDULER] Failed to enqueue backup for connection ${row.connection_id}`,
                err
            );
            await insertAuditLog({
                roleAtTime: "SYSTEM",
                actionType: "SCHEDULED_BACKUP_ENQUEUED",
                actionCategory: "BACKUP",
                resourceType: "CONNECTION",
                resourceId: row.connection_id,
                message: "Scheduled backup enqueue failed",
                status: "FAILED",
                errorMessage: err.message,
                metadata: { backupType: row.default_backup_type },
            });
        }
    }

    console.log(`[SCHEDULER] Enqueued ${enqueuedCount} backup jobs`);
}

module.exports = { runScheduledBackups };
