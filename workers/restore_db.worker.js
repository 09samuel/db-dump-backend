require("dotenv").config();
const { Worker } = require("bullmq");
const {handleRestoreDBJob} = require("../handlers/handleRestoreDBJob");
const redisConnection = require("../config/redis")

console.log("Restore worker started");

const worker = new Worker(
  "restore-db",
  async (job) => {
    console.log("JOB RECEIVED BY WORKER", job.id, job.name);
    await handleRestoreDBJob(job);
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


