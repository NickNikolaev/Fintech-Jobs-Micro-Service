const createJson = require('./createJson');
const createXls = require('./createXls');
const createXlsx = require('./createXlsx');
const logger = require('../../../../config/logger');
const createTxtAndCsv = require('./createTxtAndCsv');

const createFiles = async (context, dbConfig, jsonData) => {
  const fileContexts = [];
  const { task, taskReport, localFolder } = context;

  const promises = await task.file.format.map(async (format) => {
    const timeNow = new Date().toISOString().replace('.', '-').replace(/:/g, '-');
    const timeYesterday = new Date(new Date().getTime() - 24 * 60 * 60 * 1000)
      .toISOString()
      .replace('.', '-')
      .replace(/:/g, '-');
    const saveName = `${task.file.name ? task.file.name : 'file'}${task.file.timestamp ? timeNow : ''}${format}`;
    const nameWithoutExt = `${task.file.name ? task.file.name : 'file'}${task.file.timestamp ? timeNow : ''}`;

    taskReport.files.push({
      fileName: task.file.name + format,
      status: 'success',
      linesProcessed: 0,
      linesSucceeded: 0,
      linesErrored: 0,
      processTime: 0,
    });
    const fileReport = taskReport.files[taskReport.files.length - 1];
    const timeStart = performance.now();
    let timeEnd = 0;
    const fileFullPath = `${localFolder}/${saveName}`;

    const fileContext = {
      file: {
        type: format,
        fullPatch: fileFullPath,
        timeNow,
        timeYesterday,
        name: saveName,
        nameWithoutExt: `${localFolder}/${nameWithoutExt}`,
        nameForLog: nameWithoutExt,
      },
      ...context,
    };

    try {
      switch (format) {
        case '.txt':
        case '.csv': {
          await createTxtAndCsv(fileContext, dbConfig, fileReport, jsonData);
          logger.verbose(' == Saved as CSV/TXT file == ');
          break;
        }

        case '.xls': {
          await createXls(fileContext, dbConfig, fileReport, jsonData);
          logger.verbose(' == Saved as XLS file == ');
          break;
        }

        case '.xlsx': {
          await createXlsx(fileContext, dbConfig, fileReport, jsonData);
          logger.verbose(' == Saved as XLSX file == ');
          break;
        }

        case '.json': {
          await createJson(fileContext, dbConfig, fileReport, jsonData);
          logger.verbose(' == Saved as JSON file == ');
          break;
        }

        default:
          logger.verbose(' == Not known task type ! ==');
      }
    } catch (e) {
      fileReport.status = 'failed';
      throw new Error(e);
    }

    timeEnd = performance.now();
    fileReport.processTime = timeEnd - timeStart;

    fileContexts.push(fileContext);
  });

  await Promise.all(promises);

  return fileContexts;
};

module.exports = createFiles;
