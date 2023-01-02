const { cacheService } = require('../../shared');
const createFiles = require('./shared/createFiles');
const interpolateObject = require('../../../utils/interpolateObject');
const getDataService = require('../getData');
const appendDataToFiles = require('./shared/append');
const logger = require('../../../config/logger');
const createLogFile = require('../shared/createLogFile');
const executeResolvedPromisesInSerial = require('../../../utils/executeResolvedPromisesInSerial');

// /**
//  * Create file from multiple APIs
//  * @param context
//  * @returns {Promise<void>}
//  */
// const multipleSources = async (context) => {
//   const fileContexts = [];
//   let emptyFilesAreCreated = false;
//   const { task, dbConfig } = context;
//
//   // Get cache by task.location.cache
//   const cache = await cacheService.get(task.location.cache);
//
//   // Get every interpolation object, whose type is "location"
//   const locationInterpolationObjects = task.config.map.filter((mapObject) => mapObject.type === 'location');
//
//   // Append data to files
//   const appendDataToFilePromises = await Promise.allSettled(
//     cache.reduce((promises, cacheObject) => {
//       // Map databases config
//       const mappedDbConfig = interpolateObject(locationInterpolationObjects, cacheObject, dbConfig);
//
//       // Create promise, which appends data to files
//       const promise = new Promise((resolve, reject) =>
//         // Get data from mapped databases config
//         getDataService.api
//           .single(task, mappedDbConfig)
//           .then((apiData) => {
//             // If there isn't an api data -> Resolve
//             if (apiData.length === 0) return resolve();
//
//             // If empty files are not created -> Create them, Push them to "fileContexts" and Set "emptyFilesAreCreated" = true
//             if (!emptyFilesAreCreated) {
//               createFiles(context, mappedDbConfig, apiData)
//                 .then((emptyFileContexts) => {
//                   fileContexts.push(...emptyFileContexts);
//                   emptyFilesAreCreated = true;
//                   return resolve();
//                 })
//                 .catch(reject);
//             }
//
//             // If empty files are already created -> Append data to them
//             if (emptyFilesAreCreated)
//               appendDataToFiles(task, context.errorAdditionalInfo, fileContexts, apiData).then(resolve).catch(reject);
//           })
//           .catch(reject)
//       );
//
//       // Push promise to "promises"
//       promises.push(promise);
//       return promises;
//     }, [])
//   );
//
//   // Get all append data to file errors
//   const appendDataToFileErrors = appendDataToFilePromises
//     .filter((promise) => promise.status === 'rejected')
//     .map((promise) => promise.reason);
//
//   // TODO: Create log file with all data
//   logger.debug('fileContexts', { fileContexts });
//   // Find file context with the current file format
//   // await executeAllPromisesInParallel(fileContexts, (file, resolve, reject) =>
//   //   createLogFile(file, task).then(resolve).catch(reject)
//   // );
//
//   // If there are appended data to file errors -> Throw them
//   if (appendDataToFileErrors.length > 0) throw appendDataToFileErrors;
// };

/**
 * Create file from multiple APIs
 * @param context
 * @returns {Promise<void>}
 */
const multipleSources = async (context) => {
  const fileContexts = [];
  let emptyFilesAreCreated = false;
  const { task } = context;

  // Get cache by task.location.cache
  const cache = await cacheService.get(task.location.cache);

  // Get every interpolation object, whose type is "location"
  const locationInterpolationObjects = task.config.map.filter((mapObject) => mapObject.type === 'location');

  const appendAPIDataToFile = async (props) => {
    const { dataObject: cacheObject, dbConfig } = props;

    // Map databases config
    const mappedDbConfig = interpolateObject(locationInterpolationObjects, cacheObject, dbConfig);

    // Get data from mapped databases config
    const apiData = await getDataService.api.single(task, mappedDbConfig);

    // If there is no api data -> Return
    if (apiData.length === 0) return;

    // If empty files are not created -> Create them, Push them to "fileContexts" and Set "emptyFilesAreCreated" = true
    if (!emptyFilesAreCreated) {
      createFiles(context, mappedDbConfig, apiData).then((emptyFileContexts) => {
        fileContexts.push(...emptyFileContexts);
        emptyFilesAreCreated = true;
      });
    }

    // If empty files are already created -> Append data to them
    if (emptyFilesAreCreated) await appendDataToFiles(task, context.errorAdditionalInfo, fileContexts, apiData);
  };

  // Create empty files and Append API data to them
  await executeResolvedPromisesInSerial(cache, appendAPIDataToFile, context);

  // Create .log files
  await executeResolvedPromisesInSerial(fileContexts, (props) => createLogFile(props.dataObject.file, props.task), context);
};

module.exports = multipleSources;
