require("dotenv").config();
const { Queue } = require("bullmq");

const redisConnection = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT || 6379),
};

const backupDBQueue = new Queue("backup-db", {
  connection: redisConnection,
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
