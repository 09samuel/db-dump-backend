const { pool } = require("../db/index");
const fs = require("fs/promises");
const { deleteFromS3 } = require("../storage/delete");

async function applyKeepLastNRetention(connectionId) {
  const { rows: policyRows } = await pool.query(
    `
    SELECT retention_value
    FROM backup_settings
    WHERE connection_id = $1
      AND retention_enabled = true
      AND retention_mode = 'COUNT';
    `,
    [connectionId]
  );

  if (!policyRows.length) return;

  const policy = policyRows[0];

  const keep = policy.retention_value;
  if (!keep || keep <= 0) return;

  // fetch backups to delete
  const { rows: excess } = await pool.query(
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
    ORDER BY b.created_at DESC
    OFFSET $2;
    `,
    [connectionId, keep]
  );

  for (const backup of excess) {
    await deleteBackupSafely(backup);
  }
}


async function deleteBackupSafely(backup) {
  try {
    if (backup.storage_target === "S3") {
      if (!backup.storage_path || !backup.s3_region || !backup.backup_delete_role_arn) {
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


module.exports = { applyKeepLastNRetention }
