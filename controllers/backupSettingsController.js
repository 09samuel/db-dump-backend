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

        retention_enabled,
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
      return res.status(500).json({
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

async function updateBackupSettings(req, res) {
  try {
    const { id } = req.params;
    const {
      storageTarget,
      s3Bucket,
      s3Region,
      localStoragePath,
      retentionEnabled,
      retentionMode,
      retentionValue,
      defaultBackupType,
      schedulingEnabled,
      cronExpression,
      timeoutMinutes,
    } = req.body;

    const fields = [];
    const values = [];
    let index = 1;

    // ---------- Storage ----------
    if (storageTarget !== undefined) {
      fields.push(`storage_target = $${index++}`);
      values.push(storageTarget);

      // Clear incompatible fields
      if (storageTarget === "LOCAL") {
        fields.push(`s3_bucket = NULL`, `s3_region = NULL`);
      }

      if (storageTarget === "S3") {
        fields.push(`local_storage_path = NULL`);
      }
    }

    if (s3Bucket !== undefined) {
      fields.push(`s3_bucket = $${index++}`);
      values.push(s3Bucket);
    }

    if (s3Region !== undefined) {
      fields.push(`s3_region = $${index++}`);
      values.push(s3Region);
    }

    if (localStoragePath !== undefined) {
      fields.push(`local_storage_path = $${index++}`);
      values.push(localStoragePath);
    }

    // ---------- Retention ----------
    if (retentionEnabled !== undefined) {
      fields.push(`retention_enabled = $${index++}`);
      values.push(retentionEnabled);

      if (!retentionEnabled) {
        fields.push(`retention_mode = NULL`);
        fields.push(`retention_value = NULL`);
      }
    }

    if (retentionEnabled === true) {
      if (retentionMode !== undefined) {
        fields.push(`retention_mode = $${index++}`);
        values.push(retentionMode);
      }

      if (retentionValue !== undefined) {
        fields.push(`retention_value = $${index++}`);
        values.push(retentionValue);
      }
    }


    // ---------- Defaults ----------
    if (defaultBackupType !== undefined) {
      fields.push(`default_backup_type = $${index++}`);
      values.push(defaultBackupType);
    }

    // ---------- Scheduling ----------
    if (schedulingEnabled !== undefined) {
      fields.push(`scheduling_enabled = $${index++}`);
      values.push(schedulingEnabled);

      if (!schedulingEnabled) {
        fields.push(`cron_expression = NULL`);
      }
    }

    if (cronExpression !== undefined) {
      fields.push(`cron_expression = $${index++}`);
      values.push(cronExpression);
    }

    // ---------- Limits ----------
    if (timeoutMinutes !== undefined) {
      fields.push(`timeout_minutes = $${index++}`);
      values.push(timeoutMinutes);
    }

    if (fields.length === 0) {
      return res.status(400).json({ error: "No fields provided for update" });
    }

    // Always update timestamp
    fields.push(`updated_at = now()`);

    values.push(id);

    const query = `
      UPDATE backup_settings
      SET ${fields.join(", ")}
      WHERE connection_id = $${index}
      RETURNING *;
    `;

    const result = await pool.query(query, values);

    if (result.rowCount === 0) {
      return res.status(404).json({ error: "Backup settings not found" });
    }

    return res.status(200).json(result.rows[0]);
  } catch (error) {
    console.error("Update backup settings error:", error);
    return res
      .status(500)
      .json({ error: "Failed to update backup settings" });
  }
}



module.exports = { getBackupSettings, updateBackupSettings   };
