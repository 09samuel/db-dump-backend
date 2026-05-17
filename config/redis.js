const redisConnection = {
    host: process.env.REDIS_HOST,
    port: Number(process.env.REDIS_PORT || 6379),
    ...(process.env.NODE_ENV === 'production' && {
        tls: {},
        enableReadyCheck: false,
        maxRetriesPerRequest: null,
    })
};

module.exports = redisConnection;