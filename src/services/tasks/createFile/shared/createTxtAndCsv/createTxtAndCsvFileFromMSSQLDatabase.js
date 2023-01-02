const { parse } = require('json2csv');
const fs = require('fs');
const mapData = require('../mapData');
const logger = require('../../../../../config/logger');
const generateFromQuery = require('../../../../../utils/generateFromQuery');
const createLogFile = require('../../../shared/createLogFile');

const createTxtAndCsvFileFromMSSQLDatabase = (errorAdditionalInfo, file, task, dbConfig, endReportFile) => {
  // workaround for mssql due to issue https://github.com/knex/knex/issues/3544
  // not possible to handle at the moment
  endReportFile.linesErrored = NaN;
  endReportFile.linesSucceeded = NaN;

  let lines = 0;

  let error = false;
  const { db } = dbConfig;
  const { config } = task;

  const request = db.request();
  request.stream = true;

  const parseOptions = {
    header: config.header,
    quote: config.quotes ? config.quoteChar : '',
    delimiter: config.delimiter,
    eol: config.newLine,
  };

  // Didn't use pipeline, because this way seams to have better performance.
  // Probably it's fault how json2csv handles streams
  const output = fs.createWriteStream(file.fullPatch, {
    encoding: config.encoding && config.encoding !== '' ? config.encoding : 'utf-8',
  });

  request.on('row', (row) => {
    lines++;
    endReportFile.linesProcessed++;
    const mappedRow = mapData(
      {
        ...errorAdditionalInfo,
        file: file.fullPatch,
      },
      task,
      row
    );

    // Parse row
    let parsedRow = parse(mappedRow, parseOptions);

    // If first line -> Set parseOptions.header to false
    if (lines === 1) parseOptions.header = false;

    // If config.emptyEOL (empty end of line) -> Add empty end of line after parsed row
    if (config.emptyEOL) parsedRow += parseOptions.eol;

    // If not emptyEOL (empty end of line) and not first line -> Add end of line before parsed row
    if (!config.emptyEOL && lines > 1) parsedRow = parseOptions.eol + parsedRow;

    if (!output.write(parsedRow)) request.pause();
  });

  output.on('drain', () => request.resume());

  request.on('error', (e) => {
    error = e;
    logger.error(e, {
      ...errorAdditionalInfo,
      file: file.fullPatch,
    });
    output.end();
  });

  request.on('done', () => output.end());

  request.query(generateFromQuery(task, dbConfig));

  return new Promise((resolve, reject) => {
    output.on('finish', async () => {
      if (error) return reject(error);

      // Create .log file
      logger.debug('before creating .log file');
      logger.debug('file', file);
      logger.debug('task', task);
      await createLogFile(file, task);
      resolve();
    });
  });
};

module.exports = createTxtAndCsvFileFromMSSQLDatabase;
