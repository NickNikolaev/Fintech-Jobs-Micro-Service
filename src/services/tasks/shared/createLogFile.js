const fs = require('fs');
const { parse } = require('json2csv');
const { writeFile } = require('fs/promises');
const logger = require('../../../config/logger');
const config = require('../../../config/config');

const createLogFile = (file, task) => {
  // Read file by chunks and Get record count
  let recordCount = 0;
  fs.createReadStream(file.fullPatch)
    .on('data', (chunk) => {
      for (let i = 0; i < chunk.length; i++) if (chunk[i] === 10) recordCount++;
    })
    .on('end', () => {
      // .csv parse options
      const parseOptions = {
        header: true,
        quote: '',
        delimiter: ',',
        eol: '',
      };

      // Log json data to parse
      const logJsonData = {
        fileId: task.config.fileId,
        databaseId: task.config.databaseId,
        taskId: task.milemarkerSystemId,
        fileName: file.nameForLog,
        fileDate:
          task.config.fileDate || task.config.logDate === 'yesterday'
            ? file.timeYesterday.split('T')[0]
            : file.timeNow.split('T')[0],
        primaryKey: task.config.primaryKey,
        recordCount,
        fileType: file.type.substring(1),
        fileExtension: file.type,
        fieldDelimiter: task.config.delimiter || ',',
        skipHeader: task.config.header ? 1 : 0,
        fieldOptionallyEnclosedBy: task.config.quotes ? (task.config.quoteChar ? task.config.quoteChar : '"') : '',
        billingQuarterEndDate: task.config.parameters?.prompts ? task.config.parameters.prompts[0].defaultValue : null,
        accountAsOfDate: task.config.parameters?.prompts ? task.config.parameters.prompts[2].defaultValue : null,
      };

      // Parse json -> csv
      const logData = parse(logJsonData, parseOptions);
      logger.verbose(` = Log JSON DATA: ${JSON.stringify(logJsonData)} = `);

      // Create .log file
      return writeFile(`${config.filesFolder}/${task.file.folder}/${file.nameForLog}.log`, logData);
    });
};

module.exports = createLogFile;
