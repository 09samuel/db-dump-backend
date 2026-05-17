require("dotenv").config();
const { Queue } = require("bullmq");
const redisConnection = require("../config/redis")


const verificationQueue = new Queue("verify-connection", {
  connection: redisConnection,
});

async function enqueueVerificationJob(data, jobId) {
  await verificationQueue.add("verify", data, {jobId: jobId});
}


module.exports = { enqueueVerificationJob };
