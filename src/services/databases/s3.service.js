const AWS = require('aws-sdk');
const credentialService = require('../shared/credential.service');
const logger = require('../../config/logger');

/**
 * Create S3 instance
 * @param location
 * @returns {Promise<AWS.S3>}
 */
const createInstance = async (location) => {
  // Get AWS credentials by location.credentials
  const awsCredentials = await credentialService.getCredentialById(location.credentials);

  // Configure AWS and Return new instance of S3
  AWS.config = new AWS.Config({
    accessKeyId: awsCredentials.key,
    secretAccessKey: awsCredentials.secret,
  });
  return new AWS.S3();
};

/**
 * Check if S3 bucket exists
 * @param s3
 * @param bucketName
 * @returns {Promise<boolean>}
 */
const bucketExists = async (s3, bucketName) => {
  try {
    await s3.headBucket({ Bucket: bucketName }).promise();
    return true;
  } catch (e) {
    if (e.statusCode === 404) return false;
    logger.error(` ERROR! --- Access to bucket ${bucketName} is denied! error: ${e.message}`);
    throw new Error(` ERROR! --- Access to bucket ${bucketName} is denied! error: ${e.message}`);
  }
};

/**
 * Create new S3 bucket
 * @param s3
 * @param bucketName
 * @returns {Promise<void>}
 */
const createNewBucket = async (s3, bucketName) => {
  await s3.createBucket({ Bucket: bucketName }).promise();
  logger.verbose(` == Bucket ${bucketName} created == `);
};

module.exports = {
  createInstance,
  bucketExists,
  createNewBucket,
};
