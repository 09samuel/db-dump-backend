const express = require('express');
const router = express.Router();
const backupController = require('../controllers/backupController');
const { authenticate, checkPermission, attachConnectionIdFromBackup } = require('../middleware/authMiddleware');

router.get('/user', authenticate, backupController.getUserBackups);

router.get('/download/:backupId', authenticate, attachConnectionIdFromBackup, checkPermission("backup:read"), backupController.downloadBackup);
router.patch('/:backupId', authenticate, attachConnectionIdFromBackup, checkPermission("backup:execute"), backupController.renameBackup);
router.delete('/:backupId', authenticate, attachConnectionIdFromBackup, checkPermission("backup:execute"), backupController.deleteBackup);

router.post('/:connectionId', authenticate, checkPermission("backup:execute"), backupController.backupDB); 
router.get('/:connectionId', authenticate, checkPermission("backup:read"), backupController.getBackups);
router.get('/:connectionId/capabilities', authenticate, checkPermission("backup:read"), backupController.getBackupCapabilities);

module.exports = router;
