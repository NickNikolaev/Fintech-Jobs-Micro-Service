const { cacheService } = require('../../shared');
const createFiles = require('./shared/createFiles');
const logger = require('../../../config/logger');

/**
 * Create file from cached data
 * @param context
 * @returns {Promise<void>}
 */
const cache = async (context) => {
  // Get cache by task.location.cache
  const cachedData = await cacheService.get(context.task.location.cache);
  logger.debug('cachedData', { cachedData });

  // Create files from cache
  const dbConfig = { dbName: 'cache', connection: {} };
  await createFiles(context, dbConfig, cachedData);
};

module.exports = cache;
