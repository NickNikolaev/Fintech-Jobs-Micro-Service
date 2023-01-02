const logger = require('../../../../config/logger');

// maps columns from database
// Mapping is taken from task config
const mapData = (errorAdditionalInfo, task, data) => {
  if (!task.config || !task.config.map || !task.config.map.length) return data;
  const mapped = {};
  const headers = Object.keys(data);
  task.config.map.forEach((mapping) => {
    let { name } = mapping;

    if (!Number.isNaN(mapping.name)) {
      // if no headers data should be an array
      name = parseInt(mapping.name, 10);
      if (headers) {
        name = headers[name - 1];
      }
    }

    if (mapping.column) {
      if (data[name] !== undefined) {
        try {
          if (mapping.type) {
            switch (mapping.type) {
              case 'string':
                mapped[mapping.column] = String(data[name]);
                break;
              case 'number':
                mapped[mapping.column] = Number(data[name]);
                break;
              default:
                mapped[mapping.column] = data[name];
            }
          } else {
            mapped[mapping.column] = data[name];
          }
        } catch (err) {
          logger.error('Type conversion not successful', {
            ...errorAdditionalInfo,
            dat: data,
            error: 'Type conversion not successful',
          });
        }
      } else {
        logger.error('Bad mapping', errorAdditionalInfo);
      }
    } else if (mapping.value) {
      mapped[mapping.name] = mapping.value;
    } else {
      logger.error('Error! - Mapping should have `column` field or `value` field!', errorAdditionalInfo);
      throw new Error('Error! - Mapping should have `column` field or `value` field!', errorAdditionalInfo);
    }
  });
  return mapped;
};

module.exports = mapData;
