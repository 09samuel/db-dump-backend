require("dotenv").config();
const { Queue } = require("bullmq");
const redisConnection = require("../config/redis")

//only for N DAYS retention mode
const retentionQueue = new Queue("retention", {
  connection: redisConnection,
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
