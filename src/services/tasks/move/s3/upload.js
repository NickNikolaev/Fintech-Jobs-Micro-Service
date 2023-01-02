const { createReadStream } = require('fs');
const { unlink } = require('fs/promises');
const logger = require('../../../../config/logger');
const config = require('../../../../config/config');
const { s3Service } = require('../../../databases');
const executeResolvedPromisesInSerial = require('../../../../utils/executeResolvedPromisesInSerial');
const readFilteredFilesFromDirectory = require('../../../../utils/readFilteredFilesFromDirectory');

const uploadFile = (context) => {
  const { s3, task, location, dataObject, localFolder } = context;
  const fileName = dataObject;
  logger.verbose(` == Uploading file: ${fileName} == `);

  // Create read stream
  const filePath = `${localFolder}/${fileName}`;
  const readableStream = createReadStream(filePath, { highWaterMark: 15 * 1024 });

  // Upload to S3
  return new Promise((resolve, reject) =>
    s3
      .upload({
        Bucket: location.bucket,
        Key: `${location.bucketFolder}${fileName}`,
        Body: readableStream,
      })
      .on('httpUploadProgress', (progress) => logger.verbose(` = file upload progress: ${JSON.stringify(progress)} =`))
      .send((error) => {
        // If error -> Throw it
        if (error) reject(new Error(error));

        logger.verbose(` == File uploaded: ${fileName} == `);

        // If task.file.delete is false -> Resolve
        if (!task.file.delete) return resolve();

        // If task.file.delete is true -> Delete local file
        if (task.file.delete) {
          unlink(filePath)
            .then(() => {
              logger.verbose(` = File deleted: ${filePath} =`);
              return resolve();
            })
            .catch(reject);
        }
      })
  );
};

/**
 * Upload to S3
 * @param context
 * @returns {Promise<*[]>}
 */
const upload = async (context) => {
  logger.verbose(' == Upload to S3 == ');
  const { task, location } = context;

  // Create S3 instance
  const s3 = await s3Service.createInstance(location);

  // Check if S3 bucket exists
  const s3BucketExists = await s3Service.bucketExists(s3, location.bucket);

  // If S3 bucket doesn't exist -> Create new one
  if (!s3BucketExists) await s3Service.createNewBucket(s3, location.bucket);

  // Read directory
  const localFolder = `${config.filesFolder}/${task.file.folder}`;
  const files = await readFilteredFilesFromDirectory(task, localFolder);

  // Upload all files
  await executeResolvedPromisesInSerial(files, uploadFile, { s3, localFolder, ...context });
};

module.exports = upload;
