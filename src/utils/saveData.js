const { dynamoDBService, postgreSQLService, dbService, apiService } = require('../services/databases');

/* Saves data to database or to API
 * @task - task from mongo
 * @dbConfig - dbConfig object with database configuration options
 * @data - data after mapping
 * @errorRecorder - error logger for saving incorrect lines to file
 * @errorAdditionalInfo - additional info for handling errors
 * */
const saveData = async (context, dbConfig, data, errorRecorder, fileReport) => {
  const { errorAdditionalInfo, task } = context;

  switch (dbConfig.dbName) {
    case 'dynamoDB':
      if (!fileReport.linesUpdated) fileReport.linesUpdated = 0;
      await dynamoDBService.saveData(dbConfig, data, fileReport, errorRecorder);
      break;

    case 'redshift':
      await postgreSQLService.saveData(dbConfig, data, errorRecorder, errorAdditionalInfo, fileReport);
      break;

    case 'direct connection':
      await dbService.saveData(dbConfig, data, errorRecorder, errorAdditionalInfo, fileReport, task.config);
      break;

    case 'api':
      await apiService.saveData(task, dbConfig, data, errorRecorder, errorAdditionalInfo, fileReport, task.config);
      break;

    default:
      throw new Error(`${dbConfig.dbName} is not supported!`);
  }
};

module.exports = saveData;
