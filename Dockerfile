FROM node:18-alpine

WORKDIR /app

# Install required system packages and database tools
RUN apk add --no-cache \
    postgresql-client \
    mysql-client \
    redis \
    bash \
    curl \
    wget \
    unzip

# Install MongoDB Database Tools
RUN wget -qO /tmp/mongodb-tools.tgz \
    https://fastdl.mongodb.org/tools/db/mongodb-database-tools-alpine-x86_64-100.13.0.tgz \
 && tar -xzf /tmp/mongodb-tools.tgz -C /tmp \
 && cp /tmp/mongodb-database-tools-*/bin/mongodump /usr/local/bin/ \
 && cp /tmp/mongodb-database-tools-*/bin/mongorestore /usr/local/bin/ \
 && chmod +x /usr/local/bin/mongodump /usr/local/bin/mongorestore \
 && rm -rf /tmp/mongodb-database-tools* /tmp/mongodb-tools.tgz

# Install Node.js dependencies
COPY package*.json ./
RUN npm install --omit=dev

# Copy application code
COPY . .

# Expose application port
EXPOSE 3000

# Start application with PM2
CMD ["node_modules/.bin/pm2-runtime", "ecosystem.config.js"]        