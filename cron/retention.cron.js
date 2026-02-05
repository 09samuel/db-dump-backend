const cron = require("node-cron");
const { pool } = require("../db");
const { enqueueRetentionJob } = require("../queue/retention.queue");

async function runRetention() {
    console.log("[CRON] Retention job started");

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
      }

      console.log(`[CRON] Enqueued ${rows.length} retention jobs`);
    } catch (err) {
      console.error("[CRON] Retention cron failed", err);
    }
}

module.exports = { runRetention };
