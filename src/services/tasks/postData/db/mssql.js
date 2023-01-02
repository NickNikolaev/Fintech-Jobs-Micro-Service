const generateFromQuery = require('../../../../utils/generateFromQuery');
const { cacheService } = require('../../../shared');
const logger = require('../../../../config/logger');
const getConnection = require('../../../../utils/getConnection');

/**
 * Post data to MSSQL
 * @param context
 * @returns {Promise<void>}
 */
const mssql = async (context) => {
  logger.debug(' == Post data to MSSQL == ');
  const { task, errorAdditionalInfo, dbConfig, fileReport } = context;

  // workaround for mssql due to issue https://github.com/knex/knex/issues/3544
  // not possible to handle at the moment
  fileReport.linesErrored = NaN;
  fileReport.linesSucceeded = NaN;

  // Get database connection
  const { db } = await getConnection(context, task.location.source, false);

  // Get/Generate query and Execute it
  const query = task.query || generateFromQuery(task, dbConfig);
  const data = await db.raw(query);

  // Create mapped json data
  const mappedJsonData = data.map((item) => mapData({ ...errorAdditionalInfo }, task, item));

  // Cache mapped data
  await cacheService.set(task.milemarkerSystemId, mappedJsonData);

  fileReport.linesProcessed = data.length;
};

module.exports = mssql;
