const express = require('express');
const router = express.Router();
const backupSettingsController = require('../controllers/backupSettingsController');
const { authenticate, checkPermission } = require('../middleware/authMiddleware');

router.get('/:connectionId', authenticate, checkPermission("connection:update"), backupSettingsController.getBackupSettings);
router.patch('/:connectionId', authenticate, checkPermission("connection:update"), backupSettingsController.updateBackupSettings)

module.exports = router;