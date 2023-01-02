const XLSX = require('xlsx');
const fs = require('fs');
const ExcelJS = require('exceljs');
const mapData = require('./mapData');
const logger = require('../../../../config/logger');
const generateFromQuery = require('../../../../utils/generateFromQuery');

const XLSX_LIMIT = 1048576;

async function readXLSXFromAPI(errorAdditionalInfo, fileName, task, jsonData) {
  const { config } = task;
  const xlsxOptions = {
    skipHeader: config.header ? !config.header : true,
  };
  const mappedJsonData = jsonData.map((item) => {
    return mapData(
      {
        ...errorAdditionalInfo,
        file: fileName,
      },
      task,
      item
    );
  });

  // Check if number of columns is not exceeding maximum of xlsx
  const dataKeys = jsonData.length ? Object.keys(mappedJsonData[0]) : 0;

  if (dataKeys.length > 16384) {
    logger.error(
      ` =! To many columns! Columns from '${dataKeys[256]}' (column number 257) to '${
        dataKeys[dataKeys.length]
      }' (column number ${dataKeys.length}) will not be saved to file !=`,
      errorAdditionalInfo
    );
    mappedJsonData.forEach((line) => {
      for (let i = 16384; i < dataKeys.length; i++) {
        delete line[dataKeys[i]];
      }
    });
  }

  try {
    const wbXlsx = XLSX.utils.book_new();
    const wsXlsx = XLSX.utils.json_to_sheet(mappedJsonData, xlsxOptions);
    XLSX.utils.book_append_sheet(wbXlsx, wsXlsx, 'My Sheet');
    await XLSX.writeFile(wbXlsx, fileName, { bookType: 'xlsx' });
  } catch (error) {
    throw new Error(error);
  }
}

async function createXLSXFromDbMSSQL(errorAdditionalInfo, fileName, task, dbConfig, endReportFile) {
  let error = false;
  const { config } = task;
  const { db } = dbConfig;

  // not possible to handle at the moment
  endReportFile.linesErrored = NaN;
  endReportFile.linesSucceeded = NaN;

  const request = db.request();
  request.stream = true;
  const output = fs.createWriteStream(fileName);

  const options = {
    stream: output,
    useStyles: false,
    useSharedStrings: false,
  };

  const workbook = new ExcelJS.stream.xlsx.WorkbookWriter(options);
  const worksheet = workbook.addWorksheet('My Sheet');

  const outputStream = worksheet.stream.pipes[0];

  let lines = 0;
  let tooManyColumnsMessage = false;

  request.on('row', async (row) => {
    lines++;
    endReportFile.linesProcessed++;

    const mappedRow = mapData(
      {
        ...errorAdditionalInfo,
        file: fileName,
      },
      task,
      row
    );

    // Check if number of columns is not exceeding maximum of xlsx
    const dataKeys = Object.keys(mappedRow);
    if (dataKeys.length > 16384) {
      tooManyColumnsMessage = ` =! To many columns! Columns from '${dataKeys[16384]}' (column number 16384) to '${
        dataKeys[dataKeys.length]
      }' (column number ${dataKeys.length}) will not be saved to file !=`;
      for (let i = 16384; i < dataKeys.length; i++) {
        delete mappedRow[dataKeys[i]];
      }
    }

    if (lines === 1 && config.header) {
      worksheet.addRow(Object.keys(mappedRow)).commit();
    }

    worksheet.addRow(Object.values(mappedRow)).commit();

    if (outputStream._writableState.needDrain) {
      request.pause();
    }
  });

  outputStream.on('drain', () => {
    request.resume();
  });

  request.on('error', (e) => {
    logger.error(e, {
      ...errorAdditionalInfo,
      file: fileName,
    });
    error = e;
  });

  // TODO fix limits for XLSX for procedures
  const query = generateFromQuery(task, dbConfig, config.header ? XLSX_LIMIT - 1 : XLSX_LIMIT);
  request.query(query);

  return new Promise((resolve, reject) => {
    request.on('done', async () => {
      if (tooManyColumnsMessage) logger.error(tooManyColumnsMessage, { ...errorAdditionalInfo, file: fileName });
      await worksheet.commit();
      await workbook.commit();

      error ? reject(error) : resolve();
    });
  });
}

async function createXLSXFromDb(fileName, task, dbConfig) {
  // TODO when possible to test
}

const createXlsx = async (context, dbConfig, endReportFile, jsonData) => {
  const { errorAdditionalInfo, file, task } = context;

  // If database name is "api" or "cache" -> Read .xlsx from api/cache
  if (dbConfig.dbName === 'api' || dbConfig.dbName === 'cache')
    return readXLSXFromAPI(errorAdditionalInfo, file.fullPatch, task, jsonData);

  // If database type is "mssql" -> Create .xlsx from mssql
  if (dbConfig.connection.databaseType === 'mssql')
    return createXLSXFromDbMSSQL(errorAdditionalInfo, file.fullPatch, task, dbConfig, endReportFile);

  // If database name is not "api", "cache", "mssql" -> Create .xlsx from knex
  return createXLSXFromDb(file.fullPatch, task, dbConfig);
};

module.exports = createXlsx;
