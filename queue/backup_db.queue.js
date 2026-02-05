const { Queue } = require("bullmq");

const backupDBQueue = new Queue("backup-db", {
  connection: { host: "localhost", port: 6379 },
});

async function enqueueBackupDBJob({ jobId }) {
  await backupDBQueue.add(
    "backup",
    { jobId },
    {
      attempts: 3,
      backoff: { type: "exponential", delay: 30_000 },
      removeOnComplete: true,
      removeOnFail: false,
    }
  );
}

module.exports = { enqueueBackupDBJob }
