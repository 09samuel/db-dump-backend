const { Queue } = require("bullmq");

const backupDBQueue = new Queue("backup-db", {
  connection: { host: "localhost", port: 6379 },
});

async function enqueueBackupDBJob({ jobId, backupType, storageTarget, backupName, }) {
  await backupDBQueue.add("backup", { jobId, backupType, storageTarget, backupName });
}

module.exports = { enqueueBackupDBJob };