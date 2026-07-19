const express = require('express');
const router = express.Router();
const connectionsController = require('../controllers/connectionsController');
const { authenticate, checkPermission } = require('../middleware/authMiddleware');
const { createRateLimiter } = require('../middleware/rateLimiter');

// Rate Limiters definition (100 reqs per min)
const summaryLimiter = createRateLimiter({ windowMs: 60 * 1000, max: 100, keyPrefix: 'connections-summary' });
const basicDetailsLimiter = createRateLimiter({ windowMs: 60 * 1000, max: 100, keyPrefix: 'connection-details' });

router.post('/', authenticate, connectionsController.addConnection);
router.post('/verify-dry-run', authenticate, connectionsController.verifyConnectionDryRun);
router.post('/:connectionId/verify', authenticate, checkPermission("connection:update"), connectionsController.verifyConnection);

router.get('/summary', authenticate, summaryLimiter, connectionsController.getConnnectionsSummary);
router.get('/:connectionId/overview', authenticate, checkPermission("connection:read"), connectionsController.getConnectionOverview)
router.get('/:connectionId/basic-details', authenticate, checkPermission("connection:read"), basicDetailsLimiter, connectionsController.getConnectionBasicDetails)
router.get('/:connectionId/status', authenticate, checkPermission("connection:read"), connectionsController.getConnectionStatus);
router.get('/:connectionId', authenticate, checkPermission("connection:read"), connectionsController.getConnectionDetails);

router.patch('/:connectionId', authenticate, checkPermission("connection:update"), connectionsController.updateDatabaseDetails);

router.delete('/:connectionId', authenticate, checkPermission("connection:delete"), connectionsController.deleteConnection);

module.exports = router;