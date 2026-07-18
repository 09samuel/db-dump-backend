require('dotenv').config();
const express = require('express');
const cors = require('cors');
const helmet = require('helmet');
const cookieParser = require("cookie-parser");
const logger = require('./utils/logger');
const connectionRoutes = require('./routes/connectionRoutes');
const backupRoutes = require('./routes/backupRoutes');
const restoreRoutes = require('./routes/restoreRoutes');
const backupSettingsRoutes = require('./routes/backupSettingsRoutes');
const authRoutes = require('./routes/authRoutes');
const collaboratorRoutes = require('./routes/collaboratorRoutes');
const auditRoutes = require('./routes/auditRoutes');

// Build DATABASE_URL from existing env vars if credentials are present
if (process.env.DB_PASSWORD && process.env.DB_USER && process.env.DB_HOST && process.env.DB_PORT && process.env.DB_NAME) {
  const encodedPassword = encodeURIComponent(process.env.DB_PASSWORD);
  process.env.DATABASE_URL = `postgresql://${process.env.DB_USER}:${encodedPassword}@${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_NAME}`;
}

const app = express();
app.use(helmet());
app.use(express.json());

// HTTP Request logging middleware
app.use((req, res, next) => {
  const start = Date.now();
  res.on('finish', () => {
    const duration = Date.now() - start;
    logger.info(`${req.method} ${req.originalUrl} ${res.statusCode} ${duration}ms - ${req.ip}`);
  });
  next();
});

app.use(cookieParser());

const corsOrigin = process.env.FRONTEND_URL;
app.use(cors({
  origin: corsOrigin,
  credentials: true,
}));

// health check endpoint
app.get('/health', (req, res) => {
  res.status(200).json({
    status: 'UP',
    timestamp: new Date().toISOString()
  });
});

app.use('/auth', authRoutes);
app.use('/connections', connectionRoutes);
app.use('/backups', backupRoutes);
app.use('/restore', restoreRoutes);
app.use('/backup-settings', backupSettingsRoutes);
app.use('/collaborators', collaboratorRoutes);
app.use('/audit-logs', auditRoutes);

app.use((req, res) => {
    res.status(404).send({ error: 'Route not Found' })
});

app.use((err, req, res, next) => {
  logger.error('Unhandled server error', err);
  res.status(500).json({ error: 'Internal Server Error' });
});

module.exports = app;
