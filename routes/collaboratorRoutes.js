const express = require('express');
const router = express.Router();
const collaboratorController = require('../controllers/collaboratorController');
const { authenticate, checkPermission } = require('../middleware/authMiddleware');


router.post(
  "/connection/:connectionId",
  authenticate,
  checkPermission("user:manage"),
  collaboratorController.addCollaborator
);

router.delete(
  "/connection/:connectionId/:userId",
  authenticate,
  checkPermission("user:manage"),
  collaboratorController.removeCollaborator
);

router.get(
  "/connection/:connectionId",
  authenticate,
  checkPermission("user:manage"),
  collaboratorController.getCollaborators
);

router.patch(
  "/connection/:connectionId/:userId",
  authenticate,
  checkPermission("user:manage"),
  collaboratorController.updateRole
);

module.exports = router;