// TODO: add mapping
const fs = require('fs');
const { Transform } = require('json2csv');
const stringifyTransform = require('../stringifyTransform');
const createLogFile = require('../../../shared/createLogFile');
const logger = require('../../../../../config/logger');

const createTxtAndCsvFileFromKnex = async (file, task, dbConfig) => {
  // all databases except mssql due to issue https://github.com/knex/knex/issues/3544
  return new Promise((resolve, reject) => {
    let error = false;
    const { db, dbObject } = dbConfig;
    const { config } = task;

    const parseOptions = {
      header: config.header ? config.header : true,
      quote: config.quotes ? (config.quoteChar ? config.quoteChar : '"') : '',
      delimiter: config.delimiter ? config.delimiter : ',',
      eol: config.newLine,
    };

    const transformOpts = {
      highWaterMark: 16384,
      encoding: 'utf-8',
    };

    const output = fs.createWriteStream(file.fullPatch, { encoding: 'utf8' });
    const input = db(dbObject.objectName).stream(transformOpts);
    const json2csv = new Transform(parseOptions, transformOpts);

    output.on('finish', () => {
      error ? reject(error) : resolve();
    });

    input
      .pipe(stringifyTransform())
      .pipe(json2csv)
      .pipe(output)
      .on('error', (e) => {
        error = e;
        input.destroy();
        output.end();
      })
      .on('end', async () => {
        // Create .log file
        await createLogFile(file, task);

        logger.verbose('end');
        output.end();
      });
  });
};

module.exports = createTxtAndCsvFileFromKnex;
