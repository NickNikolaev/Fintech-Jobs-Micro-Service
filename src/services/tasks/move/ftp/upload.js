const { createReadStream } = require('fs');
const { unlink } = require('fs/promises');
const logger = require('../../../../config/logger');
const config = require('../../../../config/config');
const { ftpService } = require('../../../databases');
const readFilteredFilesFromDirectory = require('../../../../utils/readFilteredFilesFromDirectory');

/**
 * Upload to FTP
 * @param context
 * @returns {Promise<Array>}
 */
const upload = async (context) => {
  logger.verbose(' == Upload to FTP == ');
  const { location, task } = context;

  // Connect to FTP
  const ftp = await ftpService.connect(context);

  // Get local files
  const localFolder = `${config.filesFolder}/${task.file.folder}`;
  const localFiles = await readFilteredFilesFromDirectory(task, localFolder);

  // Define upload local file function
  // localFiles[1] = 'file1.txt';
  const uploadFile = (fileName) =>
    new Promise((resolve, reject) => {
      // Get local and remote paths
      const localPath = `${localFolder}/${fileName}`;
      const remotePath = `${location.folder}/${fileName}`;

      // Create read stream
      const readableStream = createReadStream(localPath);
      // readableStream.on('data', () => logger.debug('transferring chunk'));
      readableStream.on('error', logger.error);

      // Upload file
      logger.debug('before uploading');
      ftp
        .uploadFrom(readableStream, remotePath)
        .then(() => {
          logger.verbose(` == File ${localPath} is uploaded == `);
          if (!task.file.delete) return resolve();

          // If task.file.delete is true -> Delete local file
          if (task.file.delete) {
            unlink(localPath)
              .then(() => {
                logger.verbose(` == File ${localPath} is deleted == `);
                resolve();
              })
              .catch(reject);
          }
        })
        .catch((error) => {
          logger.error(error);
          reject(error);
        });
    });

  // eslint-disable-next-line no-restricted-syntax
  for await (const fileName of localFiles) {
    await uploadFile(fileName);
  }

  // Close connection
  ftp.close();
};

module.exports = upload;
