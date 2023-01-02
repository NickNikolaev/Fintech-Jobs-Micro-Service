const { getXlsxStream } = require('xlstream');
const { performance } = require('perf_hooks');
const logger = require('../../../config/logger');

const saveData = require('../../../utils/saveData');
const getConnection = require('../../../utils/getConnection');
const changeUndefinedToNull = require('./shared/changeUndefinedToNull');
const allFileColumnsExist = require('./shared/allFileColumnsExist');
const mapData = require('./shared/mapData');
const ErrorRecorder = require('../../../models/ErrorRecorder.model');

const MAX_CONNECTIONS = 10;

const xlsx = async (context, fileReport) => {
  const { file, task, errorAdditionalInfo } = context;
  const timeStart = performance.now();
  let timeEnd = 0;

  try {
    logger.verbose(' = readXLSX started = ');
    const errorRecorder = new ErrorRecorder(errorAdditionalInfo, '.xlsx', task.config);
    fileReport.fileId = errorRecorder.fileId;

    const dbConfig = await getConnection(context, task.location.source);

    // TODO Implement transform stream for axios
    const sheet = task.config.sheet ? task.config.sheet : 0;

    const xlsxStream = await getXlsxStream({
      filePath: file.fullPatch,
      withHeader: task.config.header,
      sheet,
      ignoreEmpty: true,
    });

    let chunksStarted = 0;
    let chunksCompleted = 0;
    let connections = 0;
    let promiseResolve;
    let promiseReject;
    let completed = false;
    let firstLine = true;
    let failed = false;

    xlsxStream.on('finish', () => {
      completed = true;
      if (chunksCompleted >= chunksStarted) {
        errorRecorder.closeWriteStream();

        timeEnd = performance.now();
        fileReport.processTime = timeEnd - timeStart;

        if (fileReport.linesErrored) {
          fileReport.status = 'failed';
          promiseReject('Parse XLSX failed');
        } else {
          promiseResolve();
        }
      }
    });

    return new Promise((resolve, reject) => {
      promiseResolve = resolve;
      promiseReject = reject;

      xlsxStream.on('data', async (data) => {
        if (!fileReport.linesSucceeded && fileReport.linesErrored >= errorAdditionalInfo.errorLimit) return;
        if (failed) return;

        if (firstLine) {
          firstLine = false;
          const allColumnsArePresentInFile = await allFileColumnsExist(dbConfig.dbObject, data.header, data.raw.obj, task);
          if (!allColumnsArePresentInFile) {
            logger.error('ERROR! Not all columns present in file!', errorAdditionalInfo);
            failed = true;

            errorRecorder.closeWriteStream();
            timeEnd = performance.now();
            fileReport.processTime = timeEnd - timeStart;
            fileReport.status = 'failed';

            promiseReject('ERROR! Not all columns present in file');
            return;
          }
        }

        try {
          chunksStarted++;
          connections++;
          if (connections >= MAX_CONNECTIONS) xlsxStream.pause();

          // If no headers then headers A, B, C, D...
          const dataForMapping = task.config.header ? [data.raw.obj] : [Object.values(data.raw.obj)];

          // map data
          const mappedData =
            !task.config || !task.config.map || !task.config.map.length
              ? changeUndefinedToNull(dataForMapping, dbConfig.dbObject, data.header)
              : mapData(errorAdditionalInfo, task, [data.raw.obj], data.header);

          await saveData(
            {
              task,
              errorAdditionalInfo: {
                ...errorAdditionalInfo,
                parsedLine: [data.raw.obj],
                currentLine: fileReport.linesProcessed,
              },
            },
            dbConfig,
            mappedData,
            errorRecorder,
            fileReport
          );
        } catch (e) {
          if (!fileReport.linesSucceeded && fileReport.linesErrored >= errorAdditionalInfo.errorLimit) {
            errorRecorder.closeWriteStream();
            xlsxStream.end();
            logger.error(
              ` = Reading file ${file.name} failed. Error lines exceeded error lines limit. Job will be stopped. =`,
              errorAdditionalInfo
            );

            fileReport.status = 'failed';
            timeEnd = performance.now();
            fileReport.processTime = timeEnd - timeStart;

            promiseReject(` = Parse ${file.name} failed. Number of failed lines exceeded error limit = `);
          }
        } finally {
          connections--;
          if (connections < MAX_CONNECTIONS) xlsxStream.resume();

          chunksCompleted++;

          if (chunksCompleted >= chunksStarted && completed) {
            errorRecorder.closeWriteStream();

            timeEnd = performance.now();
            fileReport.processTime = timeEnd - timeStart;

            if (fileReport.linesErrored) {
              fileReport.status = 'failed';
              promiseReject('Parse XLSX failed');
            } else {
              promiseResolve();
            }
          }
        }
      });
    });
  } catch (err) {
    logger.error(err, errorAdditionalInfo);
  }
};

module.exports = xlsx;
