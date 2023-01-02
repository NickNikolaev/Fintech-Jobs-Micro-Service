const axios = require('axios');
const logger = require('../../../config/logger');
const credentialService = require('../../shared/credential.service');

const useUrlMap = (errorAdditionalInfo, oldEndpoint, urlMap, line) => {
  let endpointUrl = oldEndpoint;

  for (let i = 0; i < urlMap.length; i++) {
    const regex = new RegExp(`{{${urlMap[i].variable}}}`, 'g');
    if (line[urlMap[i].name]) {
      endpointUrl = endpointUrl.replace(regex, line[urlMap[i].name]);
    } else {
      logger.error(` = Bad url mapping = column ${urlMap[i].name} doesn't exists in parsed file!`, errorAdditionalInfo);
    }
  }

  return endpointUrl;
};

/**
 * Save API data
 * @param task
 * @param dbConfig
 * @param data
 * @param errorRecorder
 * @param errorAdditionalInfo
 * @param endReportFile
 * @param taskConfig
 * @returns {Promise<{}>}
 */
const saveData = async (task, dbConfig, data, errorRecorder, errorAdditionalInfo, endReportFile, taskConfig) => {
  let errors = 0;
  if (!data.length) return {};

  const options = {
    method: dbConfig.method,
    headers: {},
    timeout: dbConfig.timeout || 30000,
  };

  if (dbConfig.credentialToken) options.headers.Authorization = dbConfig.credentialToken;

  for (let i = 0; i < data.length; i++) {
    options.data = data[i];
    options.url =
      taskConfig.urlMap && taskConfig.urlMap.length
        ? useUrlMap(errorAdditionalInfo, dbConfig.endpoint, taskConfig.urlMap, errorAdditionalInfo.parsedLine[i])
        : dbConfig.endpoint;

    endReportFile.linesProcessed++;
    let lineSend = false;
    let reAuthenticate = 0;
    let errorMessage;
    while (!lineSend && reAuthenticate < 3) {
      try {
        await axios(options);
        endReportFile.linesSucceeded++;
        lineSend = true;
      } catch (e) {
        // if error different than bad authentication
        errorMessage = e.message;

        if (e.response && e.response.data) {
          logger.error(errorMessage, {
            response: e.response.data,
            ...errorAdditionalInfo,
          });
        }

        if (!e.response || e.response.status !== 401) break;
        if (!dbConfig.credentialToken) break;

        logger.debug(`401 error trying to reauthenticate... try ${reAuthenticate}`);
        reAuthenticate++;
        dbConfig.credentialToken = await credentialService.getCredentialTokenByAuthentication(dbConfig.authentication);
        options.headers.Authorization = dbConfig.credentialToken;
      }
    }

    if (!lineSend) {
      errors++;
      endReportFile.linesErrored++;
      if (!endReportFile.errorFilePath) endReportFile.errorFilePath = errorRecorder.filePath + errorRecorder.fileName;
      await errorRecorder.addRowToErrorFile(errorAdditionalInfo.parsedLine[i], errorMessage);
      if (!endReportFile.linesSucceeded && endReportFile.linesErrored >= errorAdditionalInfo.errorLimit)
        throw new Error('Error lines exceeded error lines limit');
    }
  }
  if (errors) throw new Error(`${endReportFile.linesErrored} lines errored while saving to db`);
};

module.exports = saveData;
