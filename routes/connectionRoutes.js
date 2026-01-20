const express = require('express');
const router = express.Router();
const connectionsController = require('../controllers/connectionsController');

router.post('/', connectionsController.addConnection);
router.post("/verify-dry-run", connectionsController.verifyConnectionDryRun);
router.post('/:id/verify', connectionsController.verifyConnection);

router.get('/summary', connectionsController.getConnnectionsSummary);
router.get('/:id/overview', connectionsController.getConnectionOverview)
router.get('/:id/status', connectionsController.getConnectionStatus);
router.get('/:id', connectionsController.getConnectionDetails);

router.patch('/:id', connectionsController.updateDatabaseDetails);

router.delete('/:id', connectionsController.deleteConnection);

module.exports = router;