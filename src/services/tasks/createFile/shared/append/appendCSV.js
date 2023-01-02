const { parse } = require('json2csv');
const { appendFile } = require('fs/promises');
const logger = require('../../../../../config/logger');

const appendCSV = async (errorAdditionalInfo, fileDetails, task, jsonData) => {
  const { config } = task;

  const parseOptions = {
    header: config.header ? config.header : false,
    quote: config.quotes ? (config.quoteChar ? config.quoteChar : '"') : '',
    delimiter: config.delimiter ? config.delimiter : ',',
    eol: config.newLine,
  };
  // const mappedJsonData = jsonData.map((item) => {
  //   return mapData(
  //     {
  //       ...errorAdditionalInfo,
  //       file: fileDetails.fullPatch,
  //     },
  //     task,
  //     item
  //   );
  // });
  // let csvData = [];
  // if (mappedJsonData.length > 0) csvData = parse(mappedJsonData, parseOptions);
  const csvData = parse(jsonData, parseOptions);

  // Save the file
  logger.debug('csvData', { csvData });
  await appendFile(fileDetails.fullPatch, csvData);
};

module.exports = appendCSV;
