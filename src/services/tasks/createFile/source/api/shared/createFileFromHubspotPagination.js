const appendDataToFiles = require('../../../shared/append');
const createFiles = require('../../../shared/createFiles');
const getDataService = require('../../../../getData');

const createFileFromHubspotPagination = async (context) => {
  const { task, dbConfig, errorAdditionalInfo } = context;
  const defaultEndpoint = dbConfig.endpoint;

  // Get data from API
  let apiData = await getDataService.api.single(task, dbConfig, true);

  // If no API data -> Throw error
  if (apiData.length === 0) throw new Error('API data is empty');

  // Create empty files
  const fileContexts = await createFiles(context, dbConfig, []);

  // While API data has paging -> Get data from API and Append it to files
  while (apiData.paging) {
    // Get data from API
    apiData = await getDataService.api.single(task, dbConfig, true);

    // Assign new API endpoint to "dbConfig.endpoint"
    dbConfig.endpoint = `${defaultEndpoint}&after=${apiData.paging?.next?.after}`;

    // Append API data to files
    await appendDataToFiles(task, errorAdditionalInfo, fileContexts, apiData.results);
  }
};

module.exports = createFileFromHubspotPagination;
