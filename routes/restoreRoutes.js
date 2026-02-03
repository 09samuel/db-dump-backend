const express = require('express');
const router = express.Router();
const restoreController = require('../controllers/restoreController');


router.post('/:dbId/:backupId', restoreController.restoreDb); 

module.exports = router;    