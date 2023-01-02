const { parse } = require('json2csv');
const { writeFile } = require('fs/promises');
const createLogFile = require('../../../shared/createLogFile');

const createTxtAndCsvFileFromAPI = async (errorAdditionalInfo, file, task, jsonData) => {
  const { config } = task;

  const parseOptions = {
    header: config.header,
    quote: config.quotes ? config.quoteChar : '',
    delimiter: config.delimiter,
    eol: config.newLine,
  };

  // const mappedJsonData = jsonData.map((item) => {
  //   return mapData(
  //     {
  //       ...errorAdditionalInfo,
  //       file: file.fullPatch,
  //     },
  //     task,
  //     item
  //   );
  // });
  // let csvData = [];
  // if (mappedJsonData.length) csvData = parse(mappedJsonData, parseOptions);
  const csvData = parse(jsonData, parseOptions);

  // Create .csv file
  await writeFile(file.fullPatch, csvData);

  // If task.location = { source } OR { cache } -> Create .log file
  if (!task.location.source || !task.location.cache) await createLogFile(file, task);
};

module.exports = createTxtAndCsvFileFromAPI;
