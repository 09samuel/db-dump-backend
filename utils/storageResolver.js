const path = require("path");
const crypto = require("crypto");

function resolveStorageConfig(settings) {
  if (settings.storage_target === "LOCAL") {
    return {
      storageTarget: "LOCAL",
      resolvedPath: path.join(
        settings.local_storage_path,
        `backup-${Date.now()}-${crypto.randomUUID()}.dump`
      ),
    };
  }

  if (settings.storage_target === "S3") {
    return {
      storageTarget: "S3",
      s3Bucket: settings.s3_bucket,
      s3Region: settings.s3_region,
      roleArn: settings.client_role_arn,
      objectKey: `backups/${Date.now()}-${crypto.randomUUID()}.dump`,
    };
  }

  throw new Error("Unsupported storage target");
}

module.exports = { resolveStorageConfig };