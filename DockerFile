# Production image
FROM node:18-alpine

# Working directory
WORKDIR /app

# Copy package.json and install only production deps
COPY package*.json ./
RUN npm install --omit=dev

# Copy app source from builder
COPY . .

# Document the port your server listens on
EXPOSE 3000

# Start the app with PM2
CMD ["node_modules/.bin/pm2-runtime", "ecosystem.config.js"]