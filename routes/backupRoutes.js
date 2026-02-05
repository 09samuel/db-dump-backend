const express = require('express');
const router = express.Router();
const backupController = require('../controllers/backupController');


router.post('/:id', backupController.backupDB); 

router.get('/:id', backupController.getBackups)
router.get('/:id/capabilities', backupController.getBackupCapabilities);
router.get('/download/:backupId', backupController.downloadBackup)

module.exports = router;