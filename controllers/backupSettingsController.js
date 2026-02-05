const { pool } = require("../db/index");
const { computeNextRunAt } = require("../utils/cronCompute")

async function getBackupSettings(req, res) {
  try {
    const { id: connectionId } = req.params;

    const { rows } = await pool.query(
      `
      SELECT
        bs.connection_id,

        c.db_type,         

        bs.storage_target,
        bs.s3_bucket,
        bs.s3_region,
        bs.backup_upload_role_arn,
        bs.backup_restore_role_arn,
        bs.backup_delete_role_arn,
        bs.local_storage_path,

        bs.retention_enabled,
        bs.retention_mode,
        bs.retention_value,

        bs.default_backup_type,

        bs.scheduling_enabled,
        bs.cron_expression,

        bs.timeout_minutes,
        bs.created_at,
        bs.updated_at
      FROM backup_settings bs
      JOIN connections c ON c.id = bs.connection_id
      WHERE bs.connection_id = $1

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

    console.log(req.body)
    const { id } = req.params;
    const {
      storageTarget,
      s3Bucket,
      s3Region,
      backupUploadRoleArn,
      backupRestoreRoleArn,
      backupDeleteRoleArn,
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

    if (storageTarget === "S3") {
      if (!s3Bucket) {
        return res.status(400).json({
          error: "s3Bucket is required for S3 storage",
        });
      }

      if (!s3Region) {
        return res.status(400).json({
          error: "s3Region is required for S3 storage",
        });
      }

      if (!backupUploadRoleArn) {
        return res.status(400).json({
          error: "backupUploadRoleARN is required for S3 storage",
        });
      }
    }

    if (storageTarget === "LOCAL") {
      if (!localStoragePath) {
        return res.status(400).json({
          error: "Local storage path is required for LOCAL storage",
        });
      }
    }

    //cron expression must be passed to enable scheduling
    const nextRunAt = schedulingEnabled === true ? (() => {
        if (!cronExpression || !cronExpression.trim()) {
          throw new Error("CRON_REQUIRED");
        }

        const result = computeNextRunAt(cronExpression);
        if (!result) {
          throw new Error("CRON_INVALID");
        }

        return result;
      })()
    : null;


    //Check if Rentention can be set (condition: strorage S3 + delete arn)
    if (retentionEnabled === true) {

      if (!retentionMode || !retentionValue || retentionValue <= 0) {
        return res.status(400).json({
          error: "Valid retentionMode and retentionValue are required",
        });
      }

      // Storage must be S3 (either already set or being set now)
      const effectiveStorageTarget =
        storageTarget ?? (await pool.query(
          `SELECT storage_target FROM backup_settings WHERE connection_id = $1`,
          [id]
        )).rows[0]?.storage_target;

      if (effectiveStorageTarget !== "S3") {
        return res.status(400).json({
          error: "Retention can only be enabled when storage target is S3",
        });
      }

      // Delete role must exist (either already set or provided now)
      const effectiveDeleteRoleArn =
        backupDeleteRoleArn ??
        (await pool.query(
          `SELECT backup_delete_role_arn FROM backup_settings WHERE connection_id = $1`,
          [id]
        )).rows[0]?.backup_delete_role_arn;

      if (!effectiveDeleteRoleArn) {
        return res.status(400).json({
          error:
            "backupDeleteRoleArn is required to enable retention policy",
        });
      }
    }


    //Storage
    if (storageTarget !== undefined) {
      fields.push(`storage_target = $${index++}`);
      values.push(storageTarget);

      if (storageTarget === "LOCAL") {
        fields.push(
          `s3_bucket = NULL`,
          `s3_region = NULL`,
          `backup_upload_role_arn = NULL`,
          `backup_restore_role_arn = NULL`,
          `backup_delete_role_arn = NULL`
        );
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

    if (backupUploadRoleArn !== undefined) {
      fields.push(`backup_upload_role_arn = $${index++}`);
      values.push(backupUploadRoleArn);
    }

    if (backupRestoreRoleArn !== undefined) {
      fields.push(`backup_restore_role_arn = $${index++}`);
      values.push(backupRestoreRoleArn || null);
    }

    if (backupDeleteRoleArn !== undefined) {
      fields.push(`backup_delete_role_arn = $${index++}`);
      values.push(backupDeleteRoleArn || null);
    }

    if (retentionEnabled !== undefined) {
      fields.push(`retention_enabled = $${index++}`);
      values.push(retentionEnabled);

      if (retentionEnabled) {
        fields.push(`retention_mode = $${index++}`);
        values.push(retentionMode);

        fields.push(`retention_value = $${index++}`);
        values.push(retentionValue);
      } else {
        fields.push(`retention_mode = NULL`);
        fields.push(`retention_value = NULL`);
      }
    }


    // Defaults
    if (defaultBackupType !== undefined) {
      fields.push(`default_backup_type = $${index++}`);
      values.push(defaultBackupType);
    }

    // Scheduling
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

    if (nextRunAt) {
      fields.push(`next_run_at = $${index++}`);
      values.push(nextRunAt);
    }


    // Limits
    if (timeoutMinutes !== undefined) {
      fields.push(`timeout_minutes = $${index++}`);
      values.push(timeoutMinutes);
    }

    if (fields.length === 0) {
      return res.status(400).json({ error: "No fields provided for update" });
    }

    // Always update timestamp
    fields.push(`updated_at = now()`);

    const whereIndex = index;
    values.push(id);

    const query = `
      UPDATE backup_settings
      SET ${fields.join(", ")}
      WHERE connection_id = $${whereIndex}
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
