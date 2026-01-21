const { pool } = require("../db/index");

async function getBackupSettings(req, res) {
  try {
    const { id: connectionId } = req.params;

    const { rows } = await pool.query(
      `
      SELECT
        connection_id,

        storage_target,
        s3_bucket,
        s3_region,
        local_storage_path,

        retention_mode,
        retention_value,

        default_backup_type,

        scheduling_enabled,
        cron_expression,

        timeout_minutes,    

        created_at,
        updated_at
      FROM backup_settings
      WHERE connection_id = $1
      `,
      [connectionId]
    );

    if (rows.length === 0) {
      return res.status(404).json({
        error: "Backup settings not found for this connection",
      });
    }

    return res.json({
      data: rows[0],
    });
  } catch (error) {
    console.error("Get backup settings error:", error);
    return res.status(500).json({
      error: "Failed to fetch backup settings",
    });
  }
}

module.exports = { getBackupSettings };
