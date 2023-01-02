const { createReadStream } = require('fs');
const papaparse = require('papaparse');
const { performance } = require('perf_hooks');
const path = require('path');

const saveData = require('../../../utils/saveData');
const getConnection = require('../../../utils/getConnection');
const mapData = require('./shared/mapData');
const changeUndefinedToNull = require('./shared/changeUndefinedToNull');
const allFileColumnsExist = require('./shared/allFileColumnsExist');
const logger = require('../../../config/logger');
const ErrorRecorder = require('../../../models/ErrorRecorder.model');

const MAX_CONNECTIONS = 10;

const txtAndCsv = async (context, fileReport) => {
  logger.verbose(' = readCSV started = ');
  logger.debug('file context', context);
  const { file, task, errorAdditionalInfo } = context;

  const timeStart = performance.now();
  let timeEnd = 0;

  const errorRecorder = new ErrorRecorder(errorAdditionalInfo, path.extname(file.name), task.config);
  Object.assign(fileReport, { fileId: errorRecorder.fileId });

  // Connect to database
  const dbConfig = await getConnection(context, task.location.source);

  return new Promise((resolve, reject) => {
    const { config } = task;
    let chunksStarted = 0;
    let chunksCompleted = 0;
    let connections = 0;
    let erroredChunks = 0;
    let completed = false;
    let firstLine = true;
    let failed = false;
    const csvStream = createReadStream(file.fullPatch, {
      highWaterMark: 15 * 1024,
    });

    papaparse.parse(csvStream, {
      header: config.header,
      delimiter: config.delimiter,
      encoding: config.encoding,
      async chunk(results) {
        if (!fileReport.linesSucceeded && fileReport.linesErrored >= errorAdditionalInfo.errorLimit) return;
        if (failed) return;

        if (firstLine) {
          if (results.data.length === 0) return reject(new Error('No data'));

          firstLine = false;
          const allColumnsArePresentInFile = await allFileColumnsExist(
            dbConfig.dbObject,
            results.meta.fields,
            results.data[0],
            task
          );

          // If not all columns are present in file -> Throw error
          if (!allColumnsArePresentInFile) {
            logger.error('ERROR! Not all columns present in file!', errorAdditionalInfo);
            failed = true;
            errorRecorder.closeWriteStream();

            timeEnd = performance.now();
            Object.assign(fileReport, {
              status: 'failed',
              processTime: timeEnd - timeStart,
            });
            return reject(new Error('ERROR! Not all columns present in file!'));
          }
        }

        try {
          chunksStarted += 1;

          if (connections >= MAX_CONNECTIONS) csvStream.pause();
          connections += 1;

          context.mappedData =
            !task.config.map || !task.config.map.length
              ? await changeUndefinedToNull(results.data, dbConfig.dbObject, results.meta.fields)
              : mapData(errorAdditionalInfo, task, results.data, results.meta.fields);
          await saveData(
            {
              task,
              errorAdditionalInfo: {
                ...errorAdditionalInfo,
                parsedLine: results.data,
                lineNumber: fileReport.linesProcessed,
                taskId: task.milemarkerSystemId,
                fileName: file.fullPatch,
              },
            },
            dbConfig,
            context.mappedData,
            errorRecorder,
            fileReport
          );
        } catch (e) {
          if (e.message) logger.error(e.message, errorAdditionalInfo);

          erroredChunks += 1;
        } finally {
          connections -= 1;

          if (connections < MAX_CONNECTIONS) csvStream.resume();

          chunksCompleted += 1;
          if (!fileReport.linesSucceeded && fileReport.linesErrored >= errorAdditionalInfo.errorLimit) {
            logger.error(
              ` = Reading file ${file.name} failed. Error lines exceeded error lines limit. Job will be stopped. =`,
              errorAdditionalInfo
            );

            errorRecorder.closeWriteStream();

            timeEnd = performance.now();
            Object.assign(fileReport, {
              status: 'failed',
              processTime: timeEnd - timeStart,
            });

            reject(new Error(` = Parse ${file.name} failed. Number of failed lines exceeded error limit = `));
          }

          if (chunksCompleted >= chunksStarted && completed) {
            logger.verbose(` = Lines done: ${fileReport.linesProcessed}, erroredLines: ${fileReport.linesErrored} = `);

            timeEnd = performance.now();
            fileReport.processTime = timeEnd - timeStart;

            if (erroredChunks) {
              errorRecorder.closeWriteStream();

              fileReport.status = 'failed';

              logger.debug('fileReport', fileReport);
              reject(
                new Error(
                  ` = Parse ${file.name} failed. ${
                    fileReport.linesErrored
                  } lines errored. See error logs for more information = Line: ${JSON.stringify(context.mappedData)} `
                )
              );
            } else {
              fileReport.linesSucceeded = fileReport.linesProcessed - fileReport.linesErrored;
              resolve();
            }
          }
        }
      },
      complete() {
        completed = true;
      },
    });
  });
};

module.exports = txtAndCsv;
