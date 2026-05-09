require('dotenv').config();
const express = require('express');
const cors = require('cors');
const connectionRoutes = require('./routes/connectionRoutes');
const backupRoutes = require('./routes/backupRoutes');
const restoreRoutes = require('./routes/restoreRoutes');
const backupSettingsRoutes = require('./routes/backupSettingsRoutes');
const authRoutes = require('./routes/authRoutes');
const collaboratorRoutes = require('./routes/collaboratorRoutes');
const auditRoutes = require('./routes/auditRoutes');

const app = express();
app.use(express.json());

const cookieParser = require("cookie-parser");
app.use(cookieParser());

const corsOrigin = process.env.FRONTEND_URL;

app.use(cors({
  origin: corsOrigin,
  credentials: true,
}));


//health check endpoint
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
  console.error(err.stack);
  res.status(500).json({ error: 'Internal Server Error' });
});


const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});
