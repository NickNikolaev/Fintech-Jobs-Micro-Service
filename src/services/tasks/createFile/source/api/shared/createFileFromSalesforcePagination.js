const appendDataToFiles = require('../../../shared/append');
const createFiles = require('../../../shared/createFiles');
const logger = require('../../../../../../config/logger');
const getDataService = require('../../../../getData');

const createFileFromSalesforcePagination = async (context) => {
  const { task, dbConfig, taskReport, localFolder, errorAdditionalInfo } = context;
  const domain = dbConfig.endpoint.split('/')[0];
  logger.debug('domain', domain);

  try {
    // Get data from API
    let apiData = await getDataService.api.single(task, dbConfig, true);

    // If no API data -> Throw error
    if (apiData.length === 0) throw new Error('API data is empty');

    // Create empty files
    const fileContexts = await createFiles(context, taskReport, localFolder, taskErrors, dbConfig, []);

    // While API data has no "done" -> Get data from API and Append it to files
    while (!apiData.done) {
      // Get data from API
      apiData = await getDataService.api.single(task, dbConfig, true);

      // Assign new API endpoint to "dbConfig.endpoint"
      dbConfig.endpoint = `${domain}${apiData.nextRecordsUrl}`;

      // Append API data to files
      await appendDataToFiles(task, errorAdditionalInfo, fileContexts, apiData.results);
    }
  } catch (error) {
    throw new Error(error);
  }
};

module.exports = createFileFromSalesforcePagination;
