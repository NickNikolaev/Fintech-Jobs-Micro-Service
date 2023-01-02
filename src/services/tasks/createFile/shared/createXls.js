const XLSX = require('xlsx');
const getConnection = require('../../../../utils/getConnection');
const mapData = require('./mapData');
const generateFromQuery = require('../../../../utils/generateFromQuery');
const logger = require('../../../../config/logger');

const XLS_LIMIT = 65536;

const createXls = async (context, dbConfig, endReportFile, jsonData) => {
  const { errorAdditionalInfo, file, task } = context;
  const { config } = task;
  let data;
  const knexDbConfig = await getConnection(context, task.location.source, false);

  // not possible to handle at the moment
  endReportFile.linesErrored = NaN;
  endReportFile.linesSucceeded = NaN;

  // for xls we don't use streams so we can just use knex
  // If database name is "api" or "cache" -> Set "jsonData" as data
  if (knexDbConfig.dbName === 'api' || knexDbConfig.dbName === 'cache') data = jsonData;

  // If database name is not "api" -> Get database connection, Get query and Execute it
  if (knexDbConfig.dbName !== 'api' && knexDbConfig.dbName !== 'cache') {
    const { db } = knexDbConfig;

    // TODO fix limits for procedure
    const query = generateFromQuery(task, knexDbConfig, config.header ? XLS_LIMIT - 1 : XLS_LIMIT);
    data = await db.raw(query);
  }

  const xlsOptions = { skipHeader: !config.header };
  // const mappedJsonData = data.map((item) =>
  //   mapData(
  //     {
  //       ...errorAdditionalInfo,
  //       file: file.fullPatch,
  //     },
  //     task,
  //     item
  //   )
  // );
  const mappedJsonData = data;

  if (mappedJsonData.length) {
    // Check if number of columns is not exceeding maximum of xls
    const dataKeys = Object.keys(mappedJsonData[0]);
    if (dataKeys.length > 256) {
      logger.error(
        ` =! To many columns! Columns from '${dataKeys[256]}' (column number 257) to '${
          dataKeys[dataKeys.length]
        }' (column number ${dataKeys.length}) will not be saved to file !=`,
        {
          ...errorAdditionalInfo,
          file: file.fullPatch,
          data: dataKeys,
        }
      );
      mappedJsonData.forEach((line) => {
        for (let i = 256; i < dataKeys.length; i++) {
          delete line[dataKeys[i]];
        }
      });
    }
  }

  endReportFile.linesProcessed = mappedJsonData.length;

  const wbXls = XLSX.utils.book_new();
  const wsXls = await XLSX.utils.json_to_sheet(mappedJsonData, xlsOptions);
  XLSX.utils.book_append_sheet(wbXls, wsXls, 'name');
  await XLSX.writeFile(wbXls, file.fullPatch, { bookType: 'xls' });
};

module.exports = createXls;
