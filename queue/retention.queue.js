require("dotenv").config();
const { Queue } = require("bullmq");

//only for N DAYS retention mode
const redisConnection = {
  host: process.env.REDIS_HOST,
  port: Number(process.env.REDIS_PORT || 6379),
};

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
