const { Queue } = require("bullmq");

const backupDBQueue = new Queue("backup-db", {
  connection: { host: "localhost", port: 6379 },
});

async function enqueueBackupDBJob({ jobId, backupType, backupName, storageTarget, resolvedPath, s3Bucket, s3Region, backupUploadRoleARN, timeoutMinutes}) {
  await backupDBQueue.add("backup", { jobId, backupType, backupName, storageTarget, resolvedPath, s3Bucket, s3Region, backupUploadRoleARN, timeoutMinutes });
}


module.exports = { enqueueBackupDBJob };