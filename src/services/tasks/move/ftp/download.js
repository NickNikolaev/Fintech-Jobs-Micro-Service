const path = require('path');
const micromatch = require('micromatch');
const { statSync } = require('fs');
const { ftpService } = require('../../../databases');
const logger = require('../../../../config/logger');
const config = require('../../../../config/config');
const createLogFile = require('../../shared/createLogFile');
const executeAllPromisesInParallel = require('../../../../utils/executeAllPromisesInParallel');

/**
 * Download from FTP
 * @param context
 * @returns {Promise<void>}
 */
const download = async (context) => {
  const { location, task } = context;

  // Connect to FTP Client
  const ftp = await ftpService.connect(context);

  // Get remote files, which match task.file.filter
  const allFiles = await ftp.list(location.folder);
  const remoteFiles = allFiles.filter((file) => micromatch.isMatch(file.name, task.file.filter));

  // Close FTP connection
  ftp.close();

  // Define download remote file function
  const downloadFile = (remoteFile, resolve, reject) =>
    ftpService
      .connect(context)
      .then((client) => {
        // Get remote and local file paths
        const remoteFilePath = `${location.folder}/${remoteFile.name}`;
        const localFilePath = path.resolve(`${config.filesFolder}/${task.file.folder}/${remoteFile.name}`);

        // Log progress for any transfer
        client.trackProgress((info) => {
          logger.debug(info.name);
          logger.debug(info.bytesOverall);
          logger.debug(remoteFile.size);

          // If transferred size equals remote file's size && local file's size equals remote file's one -> Close client
          if (info.bytesOverall === remoteFile.size && statSync(localFilePath).size === remoteFile.size) {
            logger.verbose(` == Downloaded file ${remoteFile.name} == `);

            // Stop logging progress
            client.trackProgress();

            // Create .log file
            const timeNow = new Date().toISOString().replace('.', '-').replace(/:/g, '-');
            const nameWithoutExt = remoteFile.name
              .split('.')
              .slice(0, remoteFile.name.split('.').length - 1)
              .join('.');
            const file = {
              type: path.extname(remoteFile.name),
              fullPatch: localFilePath,
              timeNow,
              timeYesterday: new Date(new Date().getTime() - 24 * 60 * 60 * 1000)
                .toISOString()
                .replace('.', '-')
                .replace(/:/g, '-'),
              name: remoteFile.name,
              nameWithoutExt: `${config.filesFolder}/${task.file.folder}/${nameWithoutExt}`,
              nameForLog: nameWithoutExt,
              ...context,
            };
            createLogFile(file, task);

            // If task.file.delete is false -> Close client and Resolve
            if (!task.file.delete) {
              client.close();
              return resolve();
            }

            // If task.file.delete is true -> Delete remote file, Close client and Resolve
            if (task.file.delete) {
              client
                .remove(remoteFilePath)
                .then(() => {
                  logger.verbose(` == Deleted file ${remoteFilePath} == `);
                  client.close();
                  return resolve();
                })
                .catch(reject);
            }
          }
        });

        // Download file to local folder
        client.downloadTo(localFilePath, remoteFilePath).then(resolve).catch(reject);
      })
      .catch(reject);

  // Download all remote files
  await executeAllPromisesInParallel(remoteFiles, downloadFile);
};

// /**
//  * Download from FTP
//  * @param context
//  * @returns {Promise<void>}
//  */
// const download = async (context) => {
//   const { location, credentials, task } = context;
//
//   // Connect to FTP Client
//   const ftp = await dbService.ftp.connect({ credentials, location });
//
//   // Get remote file names and close FTP connection
//   const remoteFiles = await getRemoteFilesByFilter(ftp, context);
//
//   // Define download remote file function
//   const downloadRemoteFile = async (remoteFile) => {
//     // Get local and remote file paths
//     const localPath = `${config.filesFolder}/${task.file.folder}/${remoteFile.name}`;
//     const remotePath = `${location.folder}/${remoteFile.name}`;
//
//     // Create write stream
//     const writableStream = createWriteStream(localPath);
//     writableStream.on('error', logger.error);
//     writableStream.on('finish', () => {
//       logger.debug('the file is downloaded');
//       writableStream.end();
//     });
//
//     // Download file
//     await ftp.downloadTo(writableStream, remotePath);
//     writableStream.end();
//     logger.verbose(` == File ${remotePath} is downloaded == `);
//
//     // Create .log file
//     const timeNow = new Date().toISOString().replace('.', '-').replace(/:/g, '-');
//     const nameWithoutExt = remoteFile.name
//       .split('.')
//       .slice(0, remoteFile.name.split('.').length - 1)
//       .join('.');
//     const file = {
//       type: path.extname(remoteFile.name),
//       fullPatch: localPath,
//       timeNow,
//       timeYesterday: new Date(new Date().getTime() - 24 * 60 * 60 * 1000).toISOString().replace('.', '-').replace(/:/g, '-'),
//       name: remoteFile.name,
//       nameWithoutExt: `${config.filesFolder}/${task.file.folder}/${nameWithoutExt}`,
//       nameForLog: nameWithoutExt,
//       ...context,
//     };
//     createLogFile(file, task);
//
//     // If task.file.delete is true -> Delete remote file
//     if (task.file.delete) {
//       await ftp.remove(remotePath);
//       logger.verbose(` == Deleted file ${remotePath} == `);
//     }
//   };
//
//   // Download all remote files
//   await executeResolvedPromisesInSerial(remoteFiles, downloadRemoteFile);
//
//   // Close connection
//   ftp.close();
// };

module.exports = download;
