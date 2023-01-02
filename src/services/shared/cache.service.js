const { createClient } = require('redis');
require('@node-redis/json');
const logger = require('../../config/logger');
const config = require('../../config/config');

// Connect to Redis
const jobsCache = createClient({ name: 'jobs-cache:' });
jobsCache
  .connect()
  .then(() => logger.verbose(' == Connected to Redis == '))
  .catch(logger.error);

// Log Redis errors
jobsCache.on('error', (err) => logger.error('Redis Client Error', err));

/**
 * Get redis value by key
 * @param key              key
 * @returns {Promise<any>} value
 */
const get = async (key) => {
  // Get cache by key
  const cache = await jobsCache.json.get(`${config.redis.folder}:${key.toString()}`, '$');

  // If no cache -> Throw error
  if (cache.length === 0) throw new Error('No cache');

  // Return cache
  return cache;
};

/**
 * Set redis key-value pair
 * @param {Number} key
 * @param {Object} value
 * @returns {Promise<any>}
 */
const set = (key, value) => jobsCache.json.set(`${config.redis.folder}:${key.toString()}`, '$', value);

/**
 * Append array of JSONs to redis key
 * @param {Number} key
 * @param {Array} value
 * @returns {Promise<void>}
 */
const arrAppend = async (key, value) => {
  // Check if key exists in Redis
  const keyExists = await jobsCache.exists(`${config.redis.folder}:${key.toString()}`);

  // If key doesn't exist in Redis -> Set new key with value empty array
  if (!keyExists) await set(key, []);

  // Append data
  await Promise.all(
    value.map((valueObject) => jobsCache.json.arrAppend(`${config.redis.folder}:${key.toString()}`, '$', valueObject))
  );
};

/**
 * Delete redis key
 * @param {Number} key
 * @returns {Promise<any>}
 */
const del = async (key) => {
  await jobsCache.json.del(`${config.redis.folder}:${key.toString()}`, '$');
  logger.verbose(` == Redis key ${key} is deleted == `);
};

module.exports = {
  get,
  set,
  arrAppend,
  del,
};
