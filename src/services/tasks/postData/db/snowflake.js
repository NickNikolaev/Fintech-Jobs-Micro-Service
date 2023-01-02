const generateFromQuery = require('../../../../utils/generateFromQuery');
const { cacheService } = require('../../../shared');
const logger = require('../../../../config/logger');
const getConnection = require('../../../../utils/getConnection');
const { snowflakeService } = require('../../../databases');

/**
 * Post data to Snowflake
 * @param context
 * @returns {Promise<void>}
 */
const snowflake = async (context) => {
  const { task, errorAdditionalInfo, dbConfig, fileReport } = context;

  // Get database connection
  const { db } = await getConnection({ ...errorAdditionalInfo }, task.location.source, false);

  // TODO fix limits for procedure
  // If task contains query -> use it to get data from the database
  // Else -> create query via "generateFromQuery"
  const query = task.query || generateFromQuery({ ...errorAdditionalInfo }, task, dbConfig);
  const data = await snowflakeService.executeQuery(db, query);

  // Create mapped json data
  const mappedJsonData = data.map((item) => mapData({ ...errorAdditionalInfo }, task, item));

  await cacheService.set(task.milemarkerSystemId, mappedJsonData);

  fileReport.linesProcessed = data.length;
  logger.verbose(' == Saved in Redis from DB == ');
};

module.exports = snowflake;
