const express = require('express');
const router = express.Router();
const restoreController = require('../controllers/restoreController');
const { authenticate, checkPermission } = require('../middleware/authMiddleware');

router.post('/:connectionId/:backupId', authenticate, checkPermission("restore:execute"), restoreController.restoreDb); 

module.exports = router;    