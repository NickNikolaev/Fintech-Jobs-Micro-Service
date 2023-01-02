const { readFile, utils } = require('xlsx');
const { performance } = require('perf_hooks');
const logger = require('../../../config/logger');

const saveData = require('../../../utils/saveData');
const getConnection = require('../../../utils/getConnection');
const allFileColumnsExist = require('./shared/allFileColumnsExist');
const changeUndefinedToNull = require('./shared/changeUndefinedToNull');
const mapData = require('./shared/mapData');
const ErrorRecorder = require('../../../models/ErrorRecorder.model');

const xls = async (context, fileReport) => {
  logger.verbose(' == Start parsing .xls file == ');
  const { file, task, errorAdditionalInfo } = context;
  const timeStart = performance.now();
  let timeEnd = 0;

  const errorRecorder = new ErrorRecorder(errorAdditionalInfo, '.xls', task.config);
  fileReport.fileId = errorRecorder.fileId;

  try {
    const dbConfig = await getConnection(context, task.location.source);

    const sheet = task.config.sheet ? task.config.sheet : 0;
    const wb = readFile(file.fullPatch);
    const sheetsNames = Object.keys(wb.Sheets);

    let headers;
    if (task.config.header) {
      // Read first row of sheet to now the headers (Object.keys doesn't always return correct order)
      headers = utils.sheet_to_json(wb.Sheets[sheetsNames[sheet]], {
        header: 1,
      });

      headers = headers[0];
    }

    const json = utils.sheet_to_json(wb.Sheets[sheetsNames[sheet]], {
      header: task.config.header ? 0 : 1,
    });

    const allColumnsArePresentInFile = await allFileColumnsExist(dbConfig.dbObject, headers, json[0], task);
    if (!allColumnsArePresentInFile) {
      logger.error('ERROR! Not all columns present in file!', errorAdditionalInfo);
      throw new Error('ERROR! Not all columns present in file!');
    }

    // TODO: throw error in mapping
    const mappedData =
      !task.config || !task.config.map || !task.config.map.length
        ? changeUndefinedToNull(json, dbConfig.dbObject, headers)
        : mapData(errorAdditionalInfo, task, json, headers);

    await saveData(
      {
        task,
        errorAdditionalInfo: {
          ...errorAdditionalInfo,
          parsedLine: json,
        },
      },
      dbConfig,
      mappedData,
      errorRecorder,
      fileReport
    );

    errorRecorder.closeWriteStream();
    timeEnd = performance.now();
    fileReport.processTime = timeEnd - timeStart;

    logger.verbose(' == .xls file parsed == ');
  } catch (e) {
    errorRecorder.closeWriteStream();

    timeEnd = performance.now();
    fileReport.processTime = timeEnd - timeStart;
    fileReport.status = 'failed';

    throw new Error(' = Save data to location failed =');
  }
};

module.exports = xls;
