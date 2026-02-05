const { Queue } = require("bullmq");

//only for N DAYS retention mode
const retentionQueue = new Queue("retention", {
  connection: { host: "localhost", port: 6379 },
});

async function enqueueRetentionJob({ connectionId }) {
  await retentionQueue.add("apply-retention", { connectionId },
    {
      removeOnComplete: true,
      removeOnFail: false,
    }
  );
}

module.exports = { enqueueRetentionJob };
