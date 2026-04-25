const express = require("express");
const router = express.Router();
const auditController = require("../controllers/auditController");
const { authenticate, checkPermission } = require("../middleware/authMiddleware");

router.get("/user", authenticate, auditController.getUserAuditLogs);
router.get("/:connectionId", authenticate, checkPermission("audit:read"), auditController.getConnectionAuditLogs);

module.exports = router;
