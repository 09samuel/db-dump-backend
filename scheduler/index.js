require("dotenv").config();
const cron = require("node-cron");
const logger = require("../utils/logger");
const { runScheduledBackups } = require("../cron/scheduling.cron");
const { runRetention } = require("../cron/retention.cron");

logger.info("Cron started");

// Runs every 5 minutes
cron.schedule("*/1 * * * *", async () => {
  try {
    await runScheduledBackups();
  } catch (err) {
    logger.error("Scheduled backup run failed:", err);
  }
});

// Runs every day at 02:00 AM
cron.schedule("0 2 * * *", async () => {
  try {
    await runRetention();
  } catch (err) {
    logger.error("Scheduled retention run failed:", err);
  }
});
