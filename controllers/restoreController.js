const restoreService = require('../services/restoreService');
const { insertAuditLog, getRequestMeta } = require("../utils/auditLogger");
const logger = require("../utils/logger");

async function restoreDb(req, res) {
  try {
    const { connectionId, backupId } = req.params;
    const requestMeta = getRequestMeta(req);

    if (!connectionId || !backupId) {
      await insertAuditLog({
        userId: req.user?.userId || null,
        roleAtTime: req.userRole || "SYSTEM",
        actionType: "RESTORE_REQUESTED",
        actionCategory: "RESTORE",
        resourceType: "CONNECTION",
        resourceId: connectionId || null,
        message: "Restore request failed due to missing ids",
        status: "FAILED",
        errorMessage: "Missing connectionId or backupId",
        ipAddress: requestMeta.ipAddress,
        userAgent: requestMeta.userAgent,
      });
      return res.status(400).json({ message: "Missing connectionId or backupId" });
    }

    const restore = await restoreService.requestRestore(connectionId, backupId, {
      userId: req.user?.userId || null,
      roleAtTime: req.userRole || null,
      ipAddress: req.ip || null,
      userAgent: req.headers?.["user-agent"] || null,
    });

    return res.status(202).json(restore);

  } catch (err) {
    logger.error("restore db error:", err);

    return res.status(err.status || 500).json({
      error: err.message || "Internal server error"
    });
  }
}

module.exports = {restoreDb}
