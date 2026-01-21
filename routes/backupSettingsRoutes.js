const express = require('express');
const router = express.Router();
const backupSettingsController = require('../controllers/backupSettingsController');


router.get('/:id', backupSettingsController.getBackupSettings);


module.exports = router;