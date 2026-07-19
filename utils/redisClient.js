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
    eval: async (script, numKeys, key, limit, now, window) => {
      let list = store.get(key) ? JSON.parse(store.get(key)) : [];
      const limitNum = Number(limit);
      const nowNum = Number(now);
      const windowNum = Number(window);

      const minTime = nowNum - windowNum; //Calculate oldest valid timestamp
      list = list.filter(ts => ts > minTime); // Remove old timestamps

      const currentRequests = list.length;
      if (currentRequests < limitNum) { //Check if request is within limit
        list.push(nowNum); //Add current timestamp
        store.set(key, JSON.stringify(list));
        return [1, limitNum - currentRequests - 1]; // Return 1 for allowed, and remaining count
      } else {
        return [0, 0]; // Return 0 for denied, and 0 remaining count
      }
    },
    on: () => { },
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
