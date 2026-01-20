function mapConnectionSummary(row) {
  return {
    id: row.id,
    db_name: row.db_name,
    db_type: row.db_type,
    env_tag: row.env_tag,
    status: row.status,
    lastBackupAt: row.last_backup_at,
    backupStatus: row.backup_status,
    storageUsedGB: bytesToGB(row.storage_used_bytes),
  };
}

function bytesToGB(bytes) {
  return Number((bytes / (1024 ** 3)).toFixed(2));
}

module.exports = { mapConnectionSummary };