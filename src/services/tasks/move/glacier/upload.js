const crypto = require('crypto');
const { performance } = require('perf_hooks');
const { createReadStream, statSync } = require('fs');
const logger = require('../../../../config/logger');
const config = require('../../../../config/config');
const { glacierService } = require('../../../databases');
const executeResolvedPromisesInSerial = require('../../../../utils/executeResolvedPromisesInSerial');
const { reportService } = require('../../../shared');
const readFilteredFilesFromDirectory = require('../../../../utils/readFilteredFilesFromDirectory');

const uploadFile = async (context) => {
  const { glacier, localFolder, dataObject, location, taskReport } = context;
  const fileName = dataObject;
  const vaultName = location.vault;
  logger.verbose(` = Uploading file: ${fileName} =`);

  const hash = crypto.createHash('sha256');
  const timeStart = performance.now();
  let error = false;

  // Create file report
  const fileReport = reportService.createFileReport(taskReport, {
    fileName,
    status: 'success',
    processTime: 0,
  });

  // Initiate multipart upload
  const uploadId = await glacierService.initiateMultipartUpload(glacier, vaultName);

  // Create readable stream
  const readableStream = createReadStream(`${localFolder}/${fileName}`, { highWaterMark: config.glacier.uploadDataSize });
  const fileSize = statSync(`${localFolder}/${fileName}`).size;

  /*
   * send chunks of file to glacier vault
   * create promises with every event that wait for data to reach glacier
   * update checksum of file with current chunk
   * */
  let chunkNumber = 0;
  let connections = 0;
  const chunkPromises = [];

  readableStream.on('data', (chunk) => {
    // Create chunk promise, which uploads chunk of data to Glacier
    const chunkPromise = new Promise((resolve, reject) => {
      // Increase "connections" and if they reach max -> Pause readable stream
      connections += 1;
      if (connections >= config.glacier.maxConnections) readableStream.pause();

      // Increase "chunkNumber" and update hash
      chunkNumber += 1;
      hash.update(chunk);

      // Upload chunk to Glacier
      const startByteNumber = chunkNumber * config.glacier.uploadDataSize;
      const endByteNumber = chunkNumber * config.glacier.uploadDataSize + chunk.length - 1;
      const uploadParams = {
        vaultName,
        uploadId,
        range: `bytes ${startByteNumber}-${endByteNumber}/*`,
        body: chunk,
      };
      glacierService
        .uploadMultipartPart(glacier, uploadParams)
        .then(() => {
          logger.debug(
            `Bytes uploaded: ${endByteNumber + 1}, progress: ${((endByteNumber + 1) / fileSize) * 100}% uploaded`
          );

          // Decrease "connections" and if they are below max -> Resume readable stream
          connections -= 1;
          if (connections < config.glacier.maxConnections) readableStream.resume();
          resolve();
        })
        .catch((e) => {
          logger.debug(`Chunk ${uploadParams.range} errored`);

          // Destroy readable stream and Reject
          readableStream.destroy();
          endReadReject(e.message);
          reject(e);
        });
    });

    // Push chunk promise to "chunkPromises"
    chunkPromises.push(chunkPromise);
  });

  readableStream.on('error', (e) => {
    error = true;
    readableStream.end();
  });

  /*
   * wait for all on.data events to end
   * close multipart upload
   * files will be available in vault after 24h
   * create promise that waits for on.end event to end
   * */
  let endReadResolve;
  let endReadReject;

  const endReadPromise = new Promise((resolve, reject) => {
    endReadResolve = resolve;
    endReadReject = reject;
  });

  readableStream.on('end', async () => {
    try {
      await Promise.all(chunkPromises);

      const finishParams = {
        vaultName,
        uploadId,
        checksum: hash.digest('hex'),
        archiveSize: fileSize.toString(),
      };

      await glacier.completeMultipartUpload(finishParams).promise();

      endReadResolve();
    } catch (e) {
      endReadReject(e.message);
    }
  });

  /*
   * wait for all on.data events to end
   * wait for on.end event to end
   * */
  return new Promise((resolve, reject) => {
    readableStream.on('close', async () => {
      // wait for all on.data and on.end events finish
      try {
        await Promise.all(chunkPromises);
        await endReadPromise;
      } catch (e) {
        error = e.message;
      }

      const timeEnd = performance.now();
      Object.assign(fileReport, {
        status: 'success',
        processTime: timeEnd - timeStart,
      });
      if (error) {
        logger.error(` = Upload file ${fileName} failed =`);
        fileReport.status = 'failed';
        reject(new Error(error));
      }
      logger.verbose(` = File uploaded: ${fileName} =`);
      resolve();
    });
  });
};

/**
 * Upload to Glacier
 * @param context
 * @returns {Promise<*[]>}
 */
const upload = async (context) => {
  logger.verbose(' == Upload to Glacier == ');
  const { task, location } = context;

  // Create Glacier instance
  const glacier = await glacierService.createInstance(location);

  // Check if Glacier vault exists
  const glacierVaultExists = await glacierService.vaultExists(glacier, location.vault);

  // If Glacier vault doesn't exist -> Create new one
  if (!glacierVaultExists) await glacierService.createNewVault(glacier, location.vault);

  // Read directory
  const localFolder = `${config.filesFolder}/${task.file.folder}`;
  const files = await readFilteredFilesFromDirectory(task, localFolder);

  // Upload all files to Glacier
  await executeResolvedPromisesInSerial(files, uploadFile, { glacier, localFolder, ...context });
};

module.exports = upload;
