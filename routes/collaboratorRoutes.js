const express = require('express');
const router = express.Router();
const collaboratorController = require('../controllers/collaboratorController');
const { authenticate, checkPermission } = require('../middleware/authMiddleware');


router.post(
  "/connections/:connectionId/collaborators",
//   authenticate,
//   checkPermission("user:manage"),
  collaboratorController.addCollaborator
);

router.delete(
  "/connections/:connectionId/collaborators/:userId",
//   authenticate,
//   checkPermission("user:manage"),
  collaboratorController.removeCollaborator
);

router.get(
  "/connections/:connectionId/collaborators",
//   authenticate,
//   checkPermission("user:manage"),
  collaboratorController.getCollaborators
);

router.patch(
  "/connections/:connectionId/collaborators/:userId",
//   authenticate,
//   checkPermission("user:manage"),
  collaboratorController.updateRole
);