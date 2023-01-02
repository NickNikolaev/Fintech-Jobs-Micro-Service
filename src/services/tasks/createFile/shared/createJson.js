const { writeFile } = require('fs').promises;
const { createWriteStream } = require('fs');
const stream = require('stream');

const mapData = require('./mapData');
const MssqlReadableStream = require('../../../../models/mssqlReadableStream.model');
const stringifyTransform = require('./stringifyTransform');
const generateFromQuery = require('../../../../utils/generateFromQuery');

async function createJSONFromDbMSSQL(errorAdditionalInfo, filename, task, dbConfig, endReportFile) {
  const { db } = dbConfig;

  const request = db.request();
  const { config } = task;

  // not possible to handle at the moment
  endReportFile.linesErrored = NaN;
  endReportFile.linesSucceeded = NaN;
  endReportFile.linesProcessed = NaN;

  request.stream = true;

  const output = createWriteStream(filename, {
    encoding: config.encoding && config.encoding !== '' ? config.encoding : 'utf-8',
  });

  const returnPromise = new Promise((resolve, reject) => {
    stream.pipeline(new MssqlReadableStream(request, task, endReportFile), output, (err) => {
      err ? reject(err) : resolve();
    });
  });

  request.query(generateFromQuery(task, dbConfig));

  return returnPromise;
}

async function createJSONFromDb(filename, task, dbConfig) {
  return new Promise((resolve, reject) => {
    let error = false;
    const { db, dbObject } = dbConfig;

    const transformOpts = {
      highWaterMark: 1024,
      encoding: 'utf-8',
    };

    const output = createWriteStream(filename, { encoding: 'utf8' });
    const input = db(dbObject.objectName).stream(transformOpts);

    output.on('finish', () => (error ? reject(error) : resolve()));

    input
      .pipe(stringifyTransform())
      .pipe(output)
      .on('error', (e) => {
        error = e;
        output.end();
      })
      .on('end', () => {
        output.end();
      });
  });
}

module.exports = async function createJSON(context, dbConfig, endReportFile, jsonData) {
  const { errorAdditionalInfo, file, task } = context;

  // If database name is "api" or "cache" -> Create .json from api/cache
  if (dbConfig.dbName === 'api' || dbConfig.dbName === 'cache') {
    const mappedJsonData = jsonData.map((item) => {
      return mapData(
        {
          ...errorAdditionalInfo,
          file: file.fullPatch,
        },
        task,
        item
      );
    });

    const fileData = mappedJsonData.length === 0 ? '' : JSON.stringify(mappedJsonData);
    return writeFile(file.fullPatch, fileData);
  }

  // If database type is "mssql" -> Create .json from mssql
  if (dbConfig.connection.databaseType === 'mssql')
    return createJSONFromDbMSSQL(errorAdditionalInfo, file.fullPatch, task, dbConfig, endReportFile);

  // If database name is not "api", "cache", "mssql" -> Create .json from knex
  return createJSONFromDb(file.fullPatch, task, dbConfig);
};
