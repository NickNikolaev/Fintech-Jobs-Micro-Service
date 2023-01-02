const getDataService = require('../../../getData');
const createFiles = require('../../shared/createFiles');
const logger = require('../../../../../config/logger');

/**
 * Create file from API source without pagination
 * @param context
 * @returns {Promise<void>}
 */
const withoutPagination = async (context) => {
  // Get data from API
  logger.debug('inside without pagination');
  const apiData = await getDataService.api.single(context.task, context.dbConfig);

  // Create files from API data
  await createFiles(context, context.dbConfig, apiData);
};

module.exports = withoutPagination;
