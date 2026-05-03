require("dotenv").config();
const { Queue } = require("bullmq");

const redisConnection = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT || 6379),
};

const verificationQueue = new Queue("verify-connection", {
  connection: redisConnection,
});

async function enqueueVerificationJob(data, jobId) {
  await verificationQueue.add("verify", data, {jobId: jobId});
}


module.exports = { enqueueVerificationJob };
