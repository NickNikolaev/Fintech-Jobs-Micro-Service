const logger = require('../../../../config/logger');
const getConnection = require('../../../../utils/getConnection');
const generateFromQuery = require('../../../../utils/generateFromQuery');
const mapData = require('./shared/mapData');

/**
 * Get data from MSSQL
 * @param context
 * @returns {Promise<*>}
 */
const mssql = async (context) => {
  // workaround for mssql due to issue https://github.com/knex/knex/issues/3544
  // not possible to handle at the moment
  const { task, fileReport, dbConfig, errorAdditionalInfo } = context;
  fileReport.linesErrored = NaN;
  fileReport.linesSucceeded = NaN;

  // Connect to MSSQL
  const { db } = await getConnection(context, task.location.source, true);

  // Generate FROM query and Execute it
  const query = generateFromQuery(task, dbConfig);
  logger.debug(query);
  const request = db.request();
  const { recordset } = await request.query(query);

  // Map recordset data and return it
  return recordset.map((item) => mapData({ ...errorAdditionalInfo }, task, item));
};

module.exports = mssql;
