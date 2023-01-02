const { cacheService, contextService } = require('../../../shared');
const logger = require('../../../../config/logger');
const interpolateObject = require('../../../../utils/interpolateObject');
const { getDataService } = require('../..');
const executeResolvedPromisesInSerial = require('../../../../utils/executeResolvedPromisesInSerial');

/**
 * Post data to multiple APIs
 * @param context
 * @returns {Promise<void>}
 */
const multiple = async (context) => {
  logger.verbose(` == Post data to API == `);
  const { task, dbConfig, fileReport } = context;

  // Get cache by task.location.cache
  const cache = await cacheService.get(task.location.cache);

  // Get every interpolation object, whose type is "location"
  const locationInterpolationObjects = task.config.map.filter((mapObject) => mapObject.type === 'location');

  const postDataToAPI = async ({ dataObject }) => {
    // Map db config and Add it to "context"
    const mappedDbConfig = interpolateObject(locationInterpolationObjects, dataObject, dbConfig);

    // Get data from interpolated databases config
    const apiData = await getDataService.api.single(task, mappedDbConfig);
    logger.debug('apiData', apiData);
  };

  // Post data to multiple APIs
  await executeResolvedPromisesInSerial(cache, postDataToAPI, context);

  fileReport.linesProcessed = cache.length;
};

module.exports = multiple;
