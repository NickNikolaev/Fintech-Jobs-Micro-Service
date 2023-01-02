const generateFromQuery = require('../../../../utils/generateFromQuery');
const { snowflakeService } = require('../../../databases');
const mapData = require('./shared/mapData');

/**
 * Get data from Snowflake
 * @param context
 * @returns {Promise<*>}
 */
const snowflake = async (context) => {
  const { task, errorAdditionalInfo, dbConfig } = context;

  // Get database connection and execute query
  const query = generateFromQuery(task, dbConfig);
  const data = await snowflakeService.executeQuery(query);

  // Map json data and return it
  return data.map((item) => mapData({ ...errorAdditionalInfo }, task, item));
};

module.exports = snowflake;
