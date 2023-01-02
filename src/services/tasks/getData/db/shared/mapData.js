const logger = require('../../../../../config/logger');

// maps columns from database
// Mapping is taken from task config
module.exports = function mapData(errorAdditionalInfo, task, data) {
  if (!task.config || !task.config.map || !task.config.map.length) return data;

  const arrayKeys = [];
  task.config.map.forEach((mapping) => {
    if (mapping.name.includes('.') && !arrayKeys.includes(mapping.name.split('.')[0]))
      arrayKeys.push(mapping.name.split('.')[0]);
  });

  logger.debug('ARRAY KEYS: ', arrayKeys);

  const mapped = [];

  if (arrayKeys.length) {
    let i = 0;
    arrayKeys.map((arrayKey) => {
      let j = 0;
      data[arrayKey].map((item) => {
        mapped.push({});
        logger.debug(`${arrayKey}${i}`);
        const headers = Object.keys(item);
        task.config.map.forEach((mapping) => {
          let name = mapping.name.includes('.') ? mapping.name.split('.')[1] : mapping.name;

          if (!Number.isNaN(mapping.name)) {
            // if no headers data should be an array
            name = Number.parseInt(mapping.name, 10);
            if (headers) {
              name = headers[name - 1];
            }
          }

          if (mapping.column) {
            if (mapping.name.includes('.') ? item[name] !== undefined : data[name] !== undefined) {
              try {
                if (mapping.type) {
                  switch (mapping.type) {
                    case 'string':
                      mapped[i][mapping.column] = String(mapping.name.includes('.') ? item[name] : data[name]);
                      break;
                    case 'number':
                      mapped[i][mapping.column] = Number(mapping.name.includes('.') ? item[name] : data[name]);
                      break;
                    default:
                      mapped[i][mapping.column] = mapping.name.includes('.') ? item[name] : data[name];
                  }
                } else {
                  mapped[i][mapping.column] = mapping.name.includes('.') ? item[name] : data[name];
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
            mapped[i][mapping.name] = mapping.value;
          } else {
            logger.error('Error! - Mapping should have `column` field or `value` field!', errorAdditionalInfo);
            throw new Error('Error! - Mapping should have `column` field or `value` field!', errorAdditionalInfo);
          }
        });
        logger.debug(`CURRENT MAP ${j}: ${JSON.stringify(mapped)}`);
        j++;
        i++;
      });
    });
    logger.debug(`FINAL MAP ${mapped.length}: ${JSON.stringify(mapped)}`);
  } else {
    logger.debug('NO ARRAY KEYS');
    const headers = Object.keys(data);
    task.config.map.forEach((mapping) => {
      let { name } = mapping;

      if (!Number.isNaN(mapping.name)) {
        // if no headers data should be an array
        name = Number.parseInt(mapping.name, 10);
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
  }
  return mapped;
};
