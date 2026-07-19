const Redis = require('ioredis');
const redisConnectionConfig = require('../config/redis');
const logger = require('./logger');

let redisClient;

if (process.env.NODE_ENV === 'test') {
  logger.info("Initializing Mock Redis Client for Test Environment");
  const store = new Map();
  redisClient = {
    get: async (key) => {
      return store.get(key) || null;
    },
    set: async (key, value, option = null, duration = null) => {
      store.set(key, value);
      return 'OK';
    },
    del: async (key) => {
      const existed = store.has(key);
      store.delete(key);
      return existed ? 1 : 0;
    },
    keys: async (pattern) => {
      const regexPattern = new RegExp('^' + pattern.replace(/\*/g, '.*') + '$');
      return Array.from(store.keys()).filter(key => regexPattern.test(key));
    },
    on: () => {},
    quit: async () => 'OK'
  };
} else {
  redisClient = new Redis(redisConnectionConfig);

  redisClient.on('connect', () => {
    logger.info('Successfully connected to Redis');
  });

  redisClient.on('error', (err) => {
    logger.error('Redis client error:', err);
  });
}

module.exports = redisClient;
