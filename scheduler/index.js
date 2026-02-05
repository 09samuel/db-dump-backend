const cron = require("node-cron");
const { runScheduledBackups } = require("../cron/scheduling.cron");

console.log("Backup scheduler started");

cron.schedule("*/5 * * * *", async () => {
  try {
    await runScheduledBackups();
  } catch (err) {
    console.error("Scheduled backup run failed:", err);
  }
});
