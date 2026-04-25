const restoreService = require('../services/restoreService');

async function restoreDb(req, res) {
  try {
    const { connectionId, backupId } = req.params;

    if (!connectionId || !backupId) {
      return res.status(400).json({ message: "Missing connectionId or backupId" });
    }

    const restore = await restoreService.requestRestore(connectionId, backupId);

    return res.status(202).json(restore);

  } catch (err) {
    console.error("restore db error:", err);

    return res.status(err.status || 500).json({
      error: err.message || "Internal server error"
    });
  }
}

module.exports = {restoreDb}