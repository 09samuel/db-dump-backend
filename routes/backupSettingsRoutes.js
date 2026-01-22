const express = require('express');
const router = express.Router();
const backupSettingsController = require('../controllers/backupSettingsController');


router.get('/:id', backupSettingsController.getBackupSettings);
router.patch('/:id', backupSettingsController.updateBackupSettings)

module.exports = router;