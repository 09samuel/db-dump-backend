require('dotenv').config();
const { execSync } = require('child_process');
const logger = require('./utils/logger');
const app = require('./app');

// Build DATABASE_URL from existing env vars
const encodedPassword = encodeURIComponent(process.env.DB_PASSWORD);
process.env.DATABASE_URL = `postgresql://${process.env.DB_USER}:${encodedPassword}@${process.env.DB_HOST}:${process.env.DB_PORT}/${process.env.DB_NAME}`;

// push schema to RDS on startup only in production
if (process.env.NODE_ENV === 'production') {
  try {
    execSync('npx prisma db push', { stdio: 'inherit' });
    logger.info('Database schema pushed successfully');
  } catch (error) {
    logger.error('Error pushing database schema:', error);
    process.exit(1);
  }
}

const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    logger.info(`Server is running on port ${PORT}`);
});
