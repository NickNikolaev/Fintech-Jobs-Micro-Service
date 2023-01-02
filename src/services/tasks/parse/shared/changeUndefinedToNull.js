const getDBConfigColumns = require('./getDBConfigColumns');
const logger = require('../../../../config/logger');

const changeUndefinedToNull = async (data, dbObject, headers) => {
  logger.debug('inside changeUndefinedToNull');
  if (!dbObject && !headers) return data;

  let dbConfigColumns = headers;

  logger.debug('dbObject', { dbObject });
  if (dbObject) dbConfigColumns = await getDBConfigColumns(dbObject);
  logger.debug('after getDBConfigColumns', { dbConfigColumns });

  return data.map((line) => {
    if (Array.isArray(line)) {
      const newLine = [];
      for (let i = 0; i < dbConfigColumns.length; i++) {
        if (line[i] === undefined) {
          newLine[i] = null;
        } else if (line[i] === '') {
          newLine[i] = null;
        } else {
          newLine[i] = line[i];
        }
      }
      return newLine;
    }
    const newLine = {};
    for (let i = 0; i < dbConfigColumns.length; i++) {
      if (line[dbConfigColumns[i]] === undefined) {
        newLine[dbConfigColumns[i]] = null;
      } else if (line[dbConfigColumns[i]] === '') {
        newLine[dbConfigColumns[i]] = null;
      } else {
        newLine[dbConfigColumns[i]] = line[dbConfigColumns[i]];
      }
    }
    return newLine;
  });
};

module.exports = changeUndefinedToNull;
