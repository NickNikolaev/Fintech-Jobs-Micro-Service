const logger = require('../../config/logger');

const buildInsertQuery = (tableName, data) => {
  let query = `INSERT INTO ${tableName} (`;

  const columns = Object.keys(data);
  columns.forEach((column) => {
    query += `${column},`;
  });

  query = query.substring(0, query.length - 1);
  query += ') VALUES (';

  columns.forEach((column) => {
    query += `'${data[column]}',`;
  });

  query = query.substring(0, query.length - 1);
  query += ');';

  return query;
};

/**
 * Save data in PostgreSQL
 * @param dbConfig
 * @param data
 * @param errorRecorder
 * @param errorAdditionalInfo
 * @param endReportFile
 * @returns {Promise<void>}
 */
const saveData = async (dbConfig, data, errorRecorder, errorAdditionalInfo, endReportFile) => {
  const { db, tableName } = dbConfig;

  for (let i = 0; i < data.length; i++) {
    if (!endReportFile.linesSucceeded && endReportFile.linesErrored >= errorAdditionalInfo.errorLimit) {
      throw new Error('Data chunks failed.');
    }
    const query = buildInsertQuery(tableName, data[i]);

    try {
      await db.raw(query);
      endReportFile.linesSucceeded++;
    } catch (e) {
      endReportFile.linesErrored++;
      if (!endReportFile.errorFilePath) endReportFile.errorFilePath = errorRecorder.filePath + errorRecorder.fileName;
      await errorRecorder.addRowToErrorFile(data[i], e.message);
    } finally {
      logger.debug(` = Lines done: ${endReportFile.linesProcessed}, erroredLines: ${endReportFile.linesErrored} = `);
      endReportFile.linesProcessed++;
    }
  }
  if (endReportFile.linesErrored) throw new Error('Data chunks failed.');
};

module.exports = {
  saveData,
};
