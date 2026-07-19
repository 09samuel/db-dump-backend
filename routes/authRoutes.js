const express = require('express');
const router = express.Router();
const authController = require('../controllers/authController');
const { authenticate } = require('../middleware/authMiddleware');
const { createRateLimiter } = require('../middleware/rateLimiter');

// Rate Limiters definition (relaxed in test environment to avoid breaking existing integration tests)
const loginLimiter = createRateLimiter({ windowMs: 60 * 1000, max: process.env.NODE_ENV === 'test' ? 100 : 5, keyPrefix: 'login' });
const registerLimiter = createRateLimiter({ windowMs: 60 * 1000, max: process.env.NODE_ENV === 'test' ? 100 : 3, keyPrefix: 'register' });
const verifyEmailLimiter = createRateLimiter({ windowMs: 5 * 60 * 1000, max: process.env.NODE_ENV === 'test' ? 100 : 5, keyPrefix: 'verify-email' });
const passwordLimiter = createRateLimiter({ windowMs: 15 * 60 * 1000, max: process.env.NODE_ENV === 'test' ? 100 : 3, keyPrefix: 'password-reset' });

router.post('/register', registerLimiter, authController.registerUser);
router.post('/login', loginLimiter, authController.loginUser);
router.post('/refresh-token', authController.refreshTokenHandler);
router.post('/logout', authenticate, authController.logoutUser);
router.post('/verify-email', verifyEmailLimiter, authController.verifyEmail);
router.post('/forgot-password', passwordLimiter, authController.forgotPassword);
router.post('/reset-password', passwordLimiter, authController.resetPassword);
router.get('/me', authenticate, authController.me);
router.get('/user/info', authenticate, authController.getUserInfo);

module.exports = router;