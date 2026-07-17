require("dotenv").config();
const { Worker } = require("bullmq");
const {handleBackupDBJob} = require("../handlers/handleBackupDBJob");
const redisConnection = require("../config/redis")
const logger = require("../utils/logger");

logger.info("Backup worker started");

const worker = new Worker(
  "backup-db",
  async (job) => {
    logger.info(`JOB RECEIVED BY WORKER ${job.id} ${job.name}`);
    await handleBackupDBJob(job);
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


