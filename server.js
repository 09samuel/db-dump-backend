require('dotenv').config();
const express = require('express');
const cors = require('cors');
const connectionRoutes = require('./routes/connectionRoutes');
const backupRoutes = require('./routes/backupRoutes');
const backupSettingsRoutes = require('./routes/backupSettingsRoutes');

const app = express();
app.use(express.json());

app.use(cors({
  origin: 'http://localhost:5173',
  credentials: true,
}));


app.use('/connections', connectionRoutes);
app.use('/backups', backupRoutes);
app.use('/backup-settings', backupSettingsRoutes)

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