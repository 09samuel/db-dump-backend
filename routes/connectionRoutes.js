const express = require('express');
const router = express.Router();
const connectionsController = require('../controllers/connectionsController');
const { authenticate, checkPermission } = require('../middleware/authMiddleware');

router.post('/', authenticate, connectionsController.addConnection);
router.post('/:connectionId/verify-dry-run', authenticate, checkPermission("connection:update"), connectionsController.verifyConnectionDryRun);
router.post('/:connectionId/verify', authenticate, checkPermission("connection:update"), connectionsController.verifyConnection);

router.get('/summary', authenticate, connectionsController.getConnnectionsSummary);
router.get('/:connectionId/overview', authenticate, checkPermission("connection:read"), connectionsController.getConnectionOverview)
router.get('/:connectionId/basic-details', authenticate, checkPermission("connection:read"), connectionsController.getConnectionBasicDetails)
router.get('/:connectionId/status', authenticate, checkPermission("connection:read"), connectionsController.getConnectionStatus);
router.get('/:connectionId', authenticate, checkPermission("connection:read"), connectionsController.getConnectionDetails);

router.patch('/:connectionId', authenticate, checkPermission("connection:update"), connectionsController.updateDatabaseDetails);

router.delete('/:connectionId', authenticate, checkPermission("connection:delete"), connectionsController.deleteConnection);

module.exports = router;