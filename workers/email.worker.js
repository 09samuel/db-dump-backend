require("dotenv").config();
const { Worker } = require("bullmq");
const {handleEmailJob} = require("../handlers/handleEmailJob");
const redisConnection = require("../config/redis")
const logger = require("../utils/logger");

logger.info("Email worker started");

const worker = new Worker(
  "emailQueue",

  async (job) => {
    logger.info(`JOB RECEIVED BY WORKER ${job.id} ${job.name}`);
    await handleEmailJob(job);
  },
  {
    connection: redisConnection,
  }
);

worker.on("completed", (job) => {
  logger.info(`JOB COMPLETED ${job.id}`);
});

worker.on("failed", (job, err) => {
  logger.error(`JOB FAILED ${job?.id}`, err);
});

worker.on("error", (err) => {
  logger.error("WORKER ERROR", err);
});


