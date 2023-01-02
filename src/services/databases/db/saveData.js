const _ = require('lodash');
const callStoredProcedure = require('../../../utils/callStoredProcedure');
const logger = require('../../../config/logger');

// const MAX_INSERT_ROW_LIMITS = 1000
const MAX_INSERT_ROW_LIMITS = 1; // for now do line by line for showing which line has error

/*
 * Creates query like this
 * INSERT INTO [dbo].[names] ([name1], [name2], [name3], [name4], [name5], [name6]) VALUES
 * ('?', '?', '?', 'test4', 'test5', 'test6'),
 * ('test1', 'test2', 'test3', 'test4', 'test5', 'test6'),
 * ('test1', 'test2', 'test3', 'test4', 'test5','test6');
 * */
const buildInsertQuery = (objectName, parameters, data, taskConfig) => {
  parameters.sort((a, b) => a.ordinalPosition > b.ordinalPosition);
  // pick variables from params
  let params = [];

  let query = `INSERT INTO ${objectName} (`;
  if (taskConfig.map && taskConfig.map.length) {
    taskConfig.map.forEach((mapLine) => {
      params.push({
        columnName: mapLine.name || mapLine.column,
        dataType: mapLine.type || 'string',
      });
      query += ` [${mapLine.name || mapLine.column}],`;
    });
  } else {
    params = parameters.map((param) => {
      return {
        ...param,
        columnName: param.columnName,
      };
    });
    params.forEach((param) => {
      query += ` [${param.columnName}],`;
    });
  }

  query = query.substring(0, query.length - 1);
  query += ') VALUES';

  data.forEach((item) => {
    let queryRow = ' (';
    if (Array.isArray(item)) {
      for (let i = 0; i < params.length; i++) {
        if (item[i] === null || item[i] === undefined) {
          queryRow += 'null';
        } else if (typeof item[i] === 'string') {
          queryRow += `'${item[i].replace(/'/g, "''")}'`;
        } else {
          queryRow += item[i];
        }
        queryRow += ',';
      }
    } else {
      params.forEach((param) => {
        const paramName = param.columnName;
        if (item[paramName] === null) {
          // if no item of this name add null
          queryRow += 'null';
        } else if (_.includes(['decimal', 'int', 'bigint'], param.dataType)) {
          // if number is in notation 4.0E-4 we need to parse it to 0.0004
          const number = parseFloat(item[paramName]);

          // if it's number
          if (!isNaN(number)) {
            queryRow += number;
          } else if (item[paramName] === '' || item[paramName] === null) {
            queryRow += 'null';
          } else {
            queryRow += item[paramName];
          }
        } else if (typeof item[paramName] === 'string') {
          // mssql escape ''
          queryRow += `'${item[paramName].replace(/'/g, "''")}'`;
        } else {
          queryRow = queryRow.substring(0, queryRow.length - 1);
        }
        queryRow += ',';
      });
    }
    queryRow = queryRow.substring(0, queryRow.length - 1);
    queryRow += '),';

    query += queryRow;
  });

  query = query.substring(0, query.length - 1);
  query += ';';

  return query;
};

/**
 * Save data in DB
 * @param dbConfig
 * @param data
 * @param errorRecorder
 * @param errorAdditionalInfo
 * @param endReportFile
 * @param taskConfig
 * @returns {Promise<{}>}
 */
const saveData = async (dbConfig, data, errorRecorder, errorAdditionalInfo, endReportFile, taskConfig) => {
  // If data is missing -> Return {}
  if (data.length === 0) return {};

  const { db, dbObject, connection } = dbConfig;

  switch (dbObject.OBJECTTYPE) {
    case 'PROCEDURE':
    case 'FUNCTION': {
      for (let i = 0; i < data.length; i++) {
        if (!endReportFile.linesSucceeded && endReportFile.linesErrored >= errorAdditionalInfo.errorLimit)
          throw new Error('Data chunks failed.');

        try {
          await callStoredProcedure(
            db,
            connection.databaseType,
            `${dbObject.OBJECTSCHEMA}.${dbObject.OBJECTNAME}`,
            dbObject.PARAMETERSJSON,
            data[i]
          );

          endReportFile.linesSucceeded++;
        } catch (e) {
          endReportFile.linesErrored++;
          await errorRecorder.addRowToErrorFile(
            errorAdditionalInfo.parsedLine[i] ? errorAdditionalInfo.parsedLine[i] : data[i],
            e.message
          );
        } finally {
          endReportFile.linesProcessed++;
          logger.debug(` = Lines done: ${endReportFile.linesProcessed}, erroredLines: ${endReportFile.linesErrored} = `);
        }
      }

      if (endReportFile.linesErrored) throw new Error('Data chunks failed.');
      break;
    }

    case 'BASE TABLE':
    case 'VIEW': {
      const chunks = _.chunk(data, MAX_INSERT_ROW_LIMITS);
      for (let i = 0; i < chunks.length; i++) {
        const query = buildInsertQuery(
          `${dbObject.OBJECTSCHEMA}.${dbObject.OBJECTNAME}`,
          dbObject.COLUMNSJSON,
          chunks[i],
          taskConfig
        );
        // logger.debug(`query: ${query}`);
        if (!endReportFile.linesSucceeded && endReportFile.linesErrored >= errorAdditionalInfo.errorLimit)
          throw new Error('Data chunks failed.');

        try {
          await db.raw(query);
          endReportFile.linesSucceeded++;
        } catch (e) {
          endReportFile.linesErrored++;
          if (!endReportFile.errorFilePath) endReportFile.errorFilePath = errorRecorder.filePath + errorRecorder.fileName;

          let errorText = 'Unknown error when saving to databases';

          if (e && e.originalError && e.originalError.info && e.originalError.info.message) {
            errorText = e.originalError.info.message;
          } else if (e && e.message) {
            errorText = e.message;
          } else if (e) {
            errorText = e;
          }

          await errorRecorder.addRowToErrorFile(errorAdditionalInfo.parsedLine[i], errorText);
        } finally {
          endReportFile.linesProcessed++;
          logger.debug(` = Lines done: ${endReportFile.linesProcessed}`);
        }
      }

      if (endReportFile.linesErrored) throw new Error(`${endReportFile.linesErrored} lines errored while saving to db`);
      break;
    }
    default: {
      logger.error(`ObjectType '${dbObject.OBJECTTYPE}' not supported `, errorAdditionalInfo);
      throw new Error(`ObjectType '${dbObject.OBJECTTYPE}' not supported `);
    }
  }
};

module.exports = saveData;
