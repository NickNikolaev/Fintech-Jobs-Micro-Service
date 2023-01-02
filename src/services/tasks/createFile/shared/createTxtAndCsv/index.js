const createCsvFileFromAPI = require('./createTxtAndCsvFileFromAPI');
const createCsvFileFromMSSQLDatabase = require('./createTxtAndCsvFileFromMSSQLDatabase');
const createCsvFileFromKnex = require('./createTxtAndCsvFileFromKnex');

const createCsvFile = (context, dbConfig, endReportFile, jsonData) => {
  const { errorAdditionalInfo, file, task } = context;

  // If database name is "api" or "cache" -> Create .csv from api
  if (dbConfig.dbName === 'api' || dbConfig.dbName === 'cache')
    return createCsvFileFromAPI(errorAdditionalInfo, file, task, jsonData);

  // If database type is "mssql" -> Create .csv file from mssql
  if (dbConfig.connection.databaseType === 'mssql')
    return createCsvFileFromMSSQLDatabase(errorAdditionalInfo, file, task, dbConfig, endReportFile);

  // If database name is not "api", "cache" or "mssql" ->  Create .csv from knex
  return createCsvFileFromKnex(file, task, dbConfig);
};

module.exports = createCsvFile;
