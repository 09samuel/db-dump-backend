const { CronExpressionParser } = require("cron-parser");

function computeNextRunAt(cronExpression) {
  try {
    const interval = CronExpressionParser.parse(cronExpression, {
      currentDate: new Date(),
      tz: "UTC",
    });
    return interval.next().toDate();
  } catch {
    return null;
  }
}

module.exports = { computeNextRunAt }