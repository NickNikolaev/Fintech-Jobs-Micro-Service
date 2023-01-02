const logger = require('../../../../config/logger');
const axios = require('axios');
const { XMLParser } = require('fast-xml-parser');

/**
 * Get data from single API
 * @param task
 * @param dbConfig
 * @param pagination
 * @returns {Promise<*>}
 */
const single = async (task, dbConfig, pagination = false) => {
  logger.debug(' == Get data from API == ');
  const object = dbConfig.dataObject || 'data';
  const options = {
    method: dbConfig.method,
    url: dbConfig.endpoint,
    headers: dbConfig.config.headers || {},
    timeout: dbConfig.timeout || 30000,
  };

  // If dbConfig.credentialToken -> Add authorization to "options"
  if (dbConfig.credentialToken) options.headers.Authorization = dbConfig.credentialToken;

  if (task.config?.parameters) {
    // TODO: we need to build functionality for mapping
    if (task.config?.parameters?.prompts)
      task.config.parameters.prompts[2].defaultValue = new Date(new Date().getTime() - 24 * 60 * 60 * 1000)
        .toISOString()
        .split('T')[0];

    options.data = dbConfig.config.body
      ? dbConfig.config.body
      : task.config.dataSource && dbConfig.customField
      ? {
          ...task.config.parameters,
          [task.config.dataSource.map[0].locationProperty]: dbConfig.customField,
        }
      : task.config.parameters;
  } else if (dbConfig.config.body) {
    options.data = dbConfig.config.body;
  }

  // Execute API call
  logger.debug('API options: ', options);
  const apiResponse = await axios(options);

  // If API data is of type "xml" -> Parse it to "json"
  if (dbConfig.config.type === 'xml') {
    const xmlParser = new XMLParser();
    apiResponse.data = xmlParser.parse(apiResponse.data);
  }

  // Return API data
  if (pagination) return apiResponse.data;
  if (!pagination) return dbConfig.dataObject ? apiResponse.data[object] : apiResponse.data;
};

module.exports = single;
