const { cacheService, contextService } = require('../../../shared');
const getDataService = require('..');
const interpolateObject = require('../../../../utils/interpolateObject');
const executeAllPromisesInParallel = require('../../../../utils/executeAllPromisesInParallel');
const executeResolvedPromisesInSerial = require('../../../../utils/executeResolvedPromisesInSerial');

// /**
//  * Get data from multiple APIs
//  * @param dbConfig
//  * @param context
//  * @returns {Promise<*>}
//  */
// const multiple = async (dbConfig, context) => {
//   // Get cache by task.location.cache
//   const cache = await cacheService.get(context.task.location.cache);
//
//   // Define get and cache API data function
//   const getAndCacheAPIData = (cacheObject, resolve, reject) => {
//     // Get every interpolation object, whose type is "location"
//     const locationInterpolationObjects = context.task.config.map.filter((mapObject) => mapObject.type === 'location');
//
//     // Map db config
//     const mappedDbConfig = interpolateObject(locationInterpolationObjects, cacheObject, dbConfig);
//
//     // Get data from API
//     getDataService.api
//       .fromSource(mappedDbConfig, context)
//       .then((apiData) => {
//         // If there is no API data -> Resolve
//         if (apiData.length === 0) return resolve();
//
//         // Append API data to Redis
//         cacheService.arrAppend(context.task.milemarkerSystemId, apiData).then(resolve).catch(reject);
//       })
//       .catch(reject);
//   };
//
//   // Append all API data to Redis
//   await executeAllPromisesInParallel(cache, getAndCacheAPIData);
//
//   // Return cached data
//   return cacheService.get(context.task.milemarkerSystemId);
// };

const getAPIDataAndAppendItToRedis = async (context) => {
  const { task, dbConfig, dataObject } = context;
  const cacheObject = dataObject;

  // Get every interpolation object, whose type is "location"
  const locationInterpolationObjects = task.config.map.filter((mapObject) => mapObject.type === 'location');

  // Map db config and Add it to "context"
  const mappedDbConfig = interpolateObject(locationInterpolationObjects, cacheObject, dbConfig);

  // Get data from API
  const apiData = await getDataService.api.single(task, mappedDbConfig);

  // Append API data to Redis
  await cacheService.arrAppend(task.milemarkerSystemId, apiData);
};

/**
 * Get data from multiple APIs
 * @param context
 * @returns {Promise<*>}
 */
const multiple = async (context) => {
  // Get cache by task.location.cache
  const cache = await cacheService.get(context.task.location.cache);

  // Append all API data to Redis
  await executeResolvedPromisesInSerial(cache, getAPIDataAndAppendItToRedis, context);

  // Return cached data
  return cacheService.get(context.task.milemarkerSystemId);
};

module.exports = multiple;
