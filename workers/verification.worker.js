require("dotenv").config();
const { Worker } = require("bullmq");
const { handleVerificationJob } = require("../handlers/handleVerificationJob");

const redisConnection = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT || 6379),
};

console.log("Verification worker started");

new Worker(
  "verify-connection",
  async (job) => {
    await handleVerificationJob(job);
  },
  {
    connection: redisConnection,
  }
);
