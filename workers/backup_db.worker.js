require("dotenv").config();
const { Worker } = require("bullmq");
const {handleBackupDBJob} = require("../handlers/handleBackupDBJob");

const redisConnection = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT || 6379),
};

console.log("Backup worker started");

const worker = new Worker(
  "backup-db",
  async (job) => {
    console.log("JOB RECEIVED BY WORKER", job.id, job.name);
    await handleBackupDBJob(job);
  },
  {
    connection: redisConnection,
  }
);

worker.on("failed", (job, err) => {
  console.error("JOB FAILED", job?.id, err);
});

worker.on("error", (err) => {
  console.error("WORKER ERROR", err);
});


