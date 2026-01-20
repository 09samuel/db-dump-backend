const express = require('express');
const router = express.Router();
const backupController = require('../controllers/backupController');


router.post('/:id', backupController.backupDB);

router.get('/:id/capabilities', backupController.getBackupCapabilities);


module.exports = router;