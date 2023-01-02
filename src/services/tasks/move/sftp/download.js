const path = require('path');
const { createWriteStream } = require('fs');
const { sftpService } = require('../../../databases');
const logger = require('../../../../config/logger');
const config = require('../../../../config/config');
const createLogFile = require('../../shared/createLogFile');
const executeResolvedPromisesInSerial = require('../../../../utils/executeResolvedPromisesInSerial');

/**
 * Download from SFTP
 * @param context
 * @returns {Promise<void>}
 */
const download = async (context) => {
  const { location, task } = context;

  // Connect to SFTP
  const sftp = await sftpService.connect(context);

  // Get remote files
  const remoteFiles = await sftp.list(location.folder, () => task.file.filter);

  // Define download remote file function
  const downloadFile = async ({ dataObject }) => {
    const remoteFile = dataObject;

    // Get local and remote paths
    const localPath = `${config.filesFolder}/${task.file.folder}/${remoteFile.name}`;
    const remotePath = `${location.folder}/${remoteFile.name}`;

    // Create write stream
    const writableStream = createWriteStream(localPath);
    writableStream.on('error', logger.error);

    // Download remote file
    await sftp.get(remotePath, writableStream);
    logger.verbose(` == Downloaded file ${remotePath} == `);

    // Create .log file
    const timeNow = new Date().toISOString().replace('.', '-').replace(/:/g, '-');
    const timeYesterday = new Date(new Date().getTime() - 24 * 60 * 60 * 1000)
      .toISOString()
      .replace('.', '-')
      .replace(/:/g, '-');
    const remoteFileNameWithoutExtension = path.parse(remoteFile.name).name;
    const file = {
      type: path.extname(remoteFile.name),
      fullPatch: localPath,
      timeNow,
      timeYesterday,
      name: remoteFile.name,
      nameWithoutExt: `${location.folder}/${remoteFileNameWithoutExtension}`,
      nameForLog: remoteFileNameWithoutExtension,
    };
    await createLogFile(file, task);
    logger.verbose(` === Created .log file ${file.nameForLog} === `);

    // If task.file.delete is true -> Delete remote file
    if (task.file.delete) {
      await sftp.delete(remotePath);
      logger.verbose(` == Deleted ${remotePath} file == `);
    }
  };

  // Download all remote files
  await executeResolvedPromisesInSerial(remoteFiles, downloadFile, context);

  // Close connection
  sftp.end();
};

module.exports = download;
