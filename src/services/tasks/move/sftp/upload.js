const { unlink } = require('fs/promises');
const { createReadStream } = require('fs');
const logger = require('../../../../config/logger');
const { sftpService } = require('../../../databases');
const config = require('../../../../config/config');
const executeResolvedPromisesInSerial = require('../../../../utils/executeResolvedPromisesInSerial');
const readFilteredFilesFromDirectory = require('../../../../utils/readFilteredFilesFromDirectory');

/**
 * Upload to SFTP
 * @param context
 * @returns {Promise<Array>}
 */
const upload = async (context) => {
  logger.verbose(' == Upload to SFTP == ');
  const { location, task } = context;

  // Connect to SFTP
  const sftp = await sftpService.connect(context);

  // Get local files
  const localFolder = `${config.filesFolder}/${task.file.folder}`;
  const localFiles = await readFilteredFilesFromDirectory(task, localFolder);

  // Define upload local file function
  const uploadFile = async ({ dataObject }) => {
    const fileName = dataObject;

    // Get local and remote paths
    const localPath = `${localFolder}/${fileName}`;
    const remotePath = `${location.folder}/${fileName}`;

    // Create read stream
    const readableStream = createReadStream(localPath);
    readableStream.on('data', () => logger.debug('transferring chunk'));
    readableStream.on('error', logger.error);

    // Upload file
    await sftp.put(readableStream, remotePath);
    logger.verbose(` == Local file ${localPath} is uploaded == `);

    // If task.file.delete is true -> Delete local file
    if (task.file.delete) {
      await unlink(localPath);
      logger.verbose(` == Local file ${localPath} is deleted == `);
    }
  };

  // Upload all files
  await executeResolvedPromisesInSerial(localFiles, uploadFile, context);

  // Close connection
  sftp.end();
};

module.exports = upload;
