const { Queue } = require("bullmq");

const backupDBQueue = new Queue("backup-db", {
  connection: { host: "localhost", port: 6379 },
});

async function enqueueBackupDBJob(jobId) {
  await backupDBQueue.add("backup", { jobId });
}

module.exports = { enqueueBackupDBJob };