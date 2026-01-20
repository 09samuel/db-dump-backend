const { Queue } = require("bullmq");

const verificationQueue = new Queue("verify-connection", {
  connection: { host: "localhost", port: 6379 },
});

async function enqueueVerificationJob(data, jobId) {
  await verificationQueue.add("verify", data, {jobId: jobId});
}


module.exports = { enqueueVerificationJob };
