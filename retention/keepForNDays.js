const { pool } = require("../db");
const fs = require("fs/promises");
const { deleteFromS3 } = require("../storage/delete");


async function applyRetainForDays(connectionId) {
  //Load retention policy
  const { rows: policyRows } = await pool.query(
    `
    SELECT retention_value
    FROM backup_settings
    WHERE connection_id = $1
      AND retention_enabled = true
      AND retention_mode = 'DAYS';
    `,
    [connectionId]
  );

  if (!policyRows.length) return;

  const days = policyRows[0].retention_value;
  if (!days || days <= 0) return;

  //Find expired backups (skip restored ones)
  const { rows: expired } = await pool.query(
    `
    SELECT
      b.id,
      b.storage_target,
      b.storage_path,
      bs.s3_region,
      bs.backup_delete_role_arn
    FROM backups b
    JOIN backup_settings bs
      ON bs.connection_id = b.connection_id
    LEFT JOIN restores r
      ON r.backup_id = b.id
    WHERE b.connection_id = $1
      AND r.id IS NULL
      AND b.created_at < now() - ($2 || ' days')::interval
    ORDER BY b.created_at ASC;
    `,
    [connectionId, days]
  );

  if (!expired.length) return;

  //Delete safely, one-by-one
  for (const backup of expired) {
    await deleteBackupSafely(backup);
  }
}

async function deleteBackupSafely(backup) {
  try {
    if (backup.storage_target === "S3") {
      if (!backup.storage_path || !backup.s3_region || !backup.backup_delete_role_arn
      ) {
        throw new Error("Missing required S3 delete parameters");
      }

      await deleteFromS3({
        s3Path: backup.storage_path,
        region: backup.s3_region,
        roleArn: backup.backup_delete_role_arn,
      });
    } else {
      await fs.unlink(backup.storage_path);
    }

    await pool.query(`DELETE FROM backups WHERE id = $1`, [backup.id]);
  } catch (err) {
    console.error("Retention delete failed", {
      backupId: backup.id,
      error: err.message,
    });
  }
}

module.exports = { applyRetainForDays };
