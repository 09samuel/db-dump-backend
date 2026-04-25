const { pool } = require("../db");
const fs = require("fs/promises");
const { deleteFromS3 } = require("../storage/delete");
const { insertAuditLog } = require("../utils/auditLogger");


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
    WHERE b.deleted_at IS NULL
      AND b.connection_id = $1
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

    await pool.query(`UPDATE backups
        SET deleted_at = NOW(),
            delete_reason = $2
        WHERE id = $1`, 
      [backup.id, "Deleted by retention policy"]
    );

    await insertAuditLog({
      roleAtTime: "SYSTEM",
      actionType: "RETENTION_DELETE",
      actionCategory: "SYSTEM",
      resourceType: "BACKUP",
      resourceId: backup.id,
      message: "Retention deleted backup successfully",
      status: "SUCCESS",
      metadata: { storageTarget: backup.storage_target },
    });
  } catch (err) {
    console.error("Retention delete failed", {
      backupId: backup.id,
      error: err.message,
    });
    await insertAuditLog({
      roleAtTime: "SYSTEM",
      actionType: "RETENTION_DELETE",
      actionCategory: "SYSTEM",
      resourceType: "BACKUP",
      resourceId: backup.id,
      message: "Retention delete failed",
      status: "FAILED",
      errorMessage: err.message,
      metadata: { storageTarget: backup.storage_target },
    });
  }
}

module.exports = { applyRetainForDays };
