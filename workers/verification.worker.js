require("dotenv").config();
const { Worker } = require("bullmq");
const { handleVerificationJob } = require("../handlers/handleVerificationJob");
const redisConnection = require("../config/redis")
const logger = require("../utils/logger");


logger.info("Verification worker started");

new Worker(
  "verify-connection",
  async (job) => {
    await handleVerificationJob(job);
  },
  {
    connection: redisConnection,
  }
);
