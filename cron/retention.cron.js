const cron = require("node-cron");
const { pool } = require("../db");
const { enqueueRetentionJob } = require("../queue/retention.queue");
const { insertAuditLog } = require("../utils/auditLogger");
const logger = require("../utils/logger");

async function runRetention() {
    logger.info("[CRON] Retention job started");
    await insertAuditLog({
      roleAtTime: "SYSTEM",
      actionType: "RETENTION_SCAN",
      actionCategory: "SYSTEM",
      resourceType: "SCHEDULER",
      message: "Retention scan started",
      status: "SUCCESS",
    });

    try {
      const { rows } = await pool.query(`
        SELECT connection_id
        FROM backup_settings
        WHERE retention_enabled = true
          AND retention_mode = 'DAYS';
      `);

      for (const row of rows) {
        await enqueueRetentionJob({
          connectionId: row.connection_id,
        });

        await insertAuditLog({
          roleAtTime: "SYSTEM",
          actionType: "RETENTION_JOB_ENQUEUED",
          actionCategory: "SYSTEM",
          resourceType: "CONNECTION",
          resourceId: row.connection_id,
          message: "Retention job enqueued",
          status: "SUCCESS",
        });
      }

      logger.info(`[CRON] Enqueued ${rows.length} retention jobs`);
    } catch (err) {
      logger.error("[CRON] Retention cron failed", err);
      await insertAuditLog({
        roleAtTime: "SYSTEM",
        actionType: "RETENTION_SCAN",
        actionCategory: "SYSTEM",
        resourceType: "SCHEDULER",
        message: "Retention scan failed",
        status: "FAILED",
        errorMessage: err.message,
      });
    }
}

module.exports = { runRetention };
