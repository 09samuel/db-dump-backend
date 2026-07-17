require("dotenv").config();
const { Worker } = require("bullmq");
const {handleRestoreDBJob} = require("../handlers/handleRestoreDBJob");
const redisConnection = require("../config/redis")
const logger = require("../utils/logger");

logger.info("Restore worker started");

const worker = new Worker(
  "restore-db",
  async (job) => {
    logger.info(`JOB RECEIVED BY WORKER ${job.id} ${job.name}`);
    await handleRestoreDBJob(job);
  },
  {
    connection: redisConnection,
  }
);

worker.on("failed", (job, err) => {
  logger.error(`JOB FAILED ${job?.id}`, err);
});

worker.on("error", (err) => {
  logger.error("WORKER ERROR", err);
});


