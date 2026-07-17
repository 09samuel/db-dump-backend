require("dotenv").config();
const { Worker } = require("bullmq");
const { applyRetainForDays } = require("../retention/keepForNDays");
const redisConnection = require("../config/redis")
const logger = require("../utils/logger");


const worker = new Worker(
  "retention",
  async (job) => {
    const { connectionId } = job.data;
    await applyRetainForDays(connectionId);
  },
  {
    connection: redisConnection,
  }
);

worker.on("failed", (job, err) => {
  logger.error(`RETENTION JOB FAILED ${job?.id}`, err);
});

worker.on("error", (err) => {
  logger.error("RETENTION WORKER ERROR", err);
});

