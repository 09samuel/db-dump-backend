const restoreService = require('../services/restoreService');

async function restoreDb(req, res) {
  try {
    const { dbId, backupId } = req.params;

    if (!dbId || !backupId) {
      return res.status(400).json({ message: "Missing dbId or backupId" });
    }

    const restore = await restoreService.requestRestore( dbId, backupId );

    return res.status(202).json(restore);
  } catch (err) {
    console.error("restore db error:", err); 
    return res.status(500).json({
      error: "Internal server error",
    });
  }
}

module.exports = {restoreDb}