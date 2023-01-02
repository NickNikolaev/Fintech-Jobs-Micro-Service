const logger = require('../../../../config/logger');

// maps columns from file to database schema
// Mapping is taken from task config
const mapData = (context, task, data, headers) => {
  if (!task.config || !task.config.map || !task.config.map.length) return data;

  return data.map((item) => {
    const mappedItem = {};

    task.config.map.forEach((mapping) => {
      let { column } = mapping;

      if (!Number.isNaN(mapping.column)) {
        column = Number.parseInt(mapping.column, 10);

        if (headers) {
          column = headers[column - 1];
        } else {
          const rowColumnNames = Object.keys(item);
          column = rowColumnNames[column - 1];
        }
      }

      if (mapping.name) {
        if (item[column] !== undefined && item[column] !== '') {
          try {
            if (mapping.type) {
              switch (mapping.type) {
                case 'string':
                  mappedItem[mapping.name] = String(item[column]);
                  break;
                case 'number':
                  mappedItem[mapping.name] = Number(item[column]);
                  break;
                default:
                  mappedItem[mapping.name] = item[column];
              }
            } else {
              mappedItem[mapping.name] = item[column];
            }
          } catch (err) {
            logger.error(' = Type conversion not successful =', context.errorAdditionalInfo);
            throw new Error(' = Type conversion not successful =', context.errorAdditionalInfo);
          }
        } else {
          mappedItem[mapping.name] = null;
        }
      } else if (mapping.value) {
        mappedItem[column] = mapping.value;
      } else {
        logger.error('Error! - Mapping should have `name` field or `value` field!', context.errorAdditionalInfo);
        throw new Error('Error! - Mapping should have `name` field or `value` field!', context.errorAdditionalInfo);
      }
    });

    return mappedItem;
  });
};

module.exports = mapData;
