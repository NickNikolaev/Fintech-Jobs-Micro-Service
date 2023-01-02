const logger = require('../../../../config/logger');
const getDBConfigColumns = require('./getDBConfigColumns');

const allFileColumnsExist = async (dbObject, fileColumns, firstLine, task) => {
  // for redshift and dynamodb
  if (!dbObject) return true;

  // skip this check when mapping
  if (task && task.config && task.config.map && task.config.map.length > 0) return true;

  const dbConfigColumns = await getDBConfigColumns(dbObject);
  logger.debug('dbConfigColumns', dbConfigColumns);
  logger.debug('first line', { firstLine });
  if (fileColumns && fileColumns.length > 0) {
    if (fileColumns.length === dbConfigColumns.length && !fileColumns.some((v) => dbConfigColumns.indexOf(v) < 0))
      return true;
  } else if (Array.isArray(firstLine)) {
    if (firstLine.length === dbConfigColumns.length) return true;
  } else if (Object.keys(firstLine).length === dbConfigColumns.length) return true;

  return false;
};

module.exports = allFileColumnsExist;
