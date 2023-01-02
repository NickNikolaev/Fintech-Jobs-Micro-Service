const { createReadStream } = require('fs');
const { performance } = require('perf_hooks');

const ErrorRecorder = require('../../../models/ErrorRecorder.model');
const saveData = require('../../../utils/saveData');
const getConnection = require('../../../utils/getConnection');
const mapData = require('./shared/mapData');
const changeUndefinedToNull = require('./shared/changeUndefinedToNull');
const logger = require('../../../config/logger');

const MAX_CONNECTIONS = 100;

const parseChunkToJson = (chunk, passToNextChunk) => {
  let stringData = '';
  const string = chunk.toString();
  if (string[0] === ',' && string[1] === '{') stringData = `[${string.slice(1)}`;
  else if (passToNextChunk.unsavedStringPart === ',') stringData = `[${string}`;
  else {
    // remove ','
    passToNextChunk.unsavedStringPart = passToNextChunk.unsavedStringPart.slice(1);
    stringData = passToNextChunk.unsavedStringPart === '' ? string : `[${passToNextChunk.unsavedStringPart}${string}`;
  }
  if (stringData.slice(-1) !== ']') {
    const indexOfLastParenthesis = stringData.lastIndexOf('}');
    passToNextChunk.unsavedStringPart = stringData.slice(indexOfLastParenthesis + 1);
    stringData = `${stringData.slice(0, indexOfLastParenthesis + 1)}]`;
  }
  return JSON.parse(stringData);
};

const json = async (context, endReportFile) => {
  const { file, task, errorAdditionalInfo } = context;
  const timeStart = performance.now();
  let timeEnd = 0;

  logger.verbose(' = readJSON started = ');
  let error = false;
  const errorRecorder = new ErrorRecorder(errorAdditionalInfo, '.json', task.config);
  endReportFile.fileId = errorRecorder.fileId;

  const dbConfig = await getConnection(context, task.location.source);
  let connections = 0;

  const readStream = createReadStream(file.fullPatch, {
    highWaterMark: 15 * 1024,
  });
  const readStreamPromises = [];
  const passToNextChunk = { unsavedStringPart: '' };

  readStream.on('data', async (chunk) => {
    readStreamPromises.push(
      new Promise((resolve) => {
        connections++;
        if (connections >= MAX_CONNECTIONS) readStream.pause();

        const jsonData = parseChunkToJson(chunk, passToNextChunk);

        const mappedJsonData =
          !task.config || !task.config.map || !task.config.map.length
            ? changeUndefinedToNull(jsonData, dbConfig.dbObject)
            : mapData(errorAdditionalInfo, task, jsonData);

        saveData(
          {
            task,
            errorAdditionalInfo: {
              ...errorAdditionalInfo,
              parsedLine: jsonData,
              taskId: task.milemarkerSystemId,
              fileName: file.fullPatch,
            },
          },
          dbConfig,
          mappedJsonData,
          errorRecorder,
          endReportFile
        )
          .catch((e) => {
            if (!e.erroredLines) {
              error = true;
              readStream.close();
            }
            if (!endReportFile.linesSucceeded && endReportFile.linesErrored >= errorAdditionalInfo.errorLimit) {
              logger.error(
                ` = Reading file ${file.name} failed. Error lines exceeded error lines limit. Job will be stopped. =`,
                errorAdditionalInfo
              );
              readStream.close();
            }
          })
          .finally(() => {
            connections--;
            if (connections < MAX_CONNECTIONS) readStream.resume();
            resolve(true);
          });
      })
    );
  });

  readStream.on('error', (e) => {
    error = true;
    readStream.end();
  });

  return new Promise((resolve, reject) => {
    readStream.on('close', async () => {
      await Promise.all(readStreamPromises);

      timeEnd = performance.now();
      endReportFile.processTime = timeEnd - timeStart;
      if (error || endReportFile.linesErrored) {
        errorRecorder.closeWriteStream();
        endReportFile.status = 'failed';
        reject(new Error('Reading json file failed'));
      }
      resolve();
    });
  });
};

module.exports = json;
