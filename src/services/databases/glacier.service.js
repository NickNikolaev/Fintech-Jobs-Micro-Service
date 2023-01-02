const AWS = require('aws-sdk');
const credentialService = require('../shared/credential.service');
const logger = require('../../config/logger');
const config = require('../../config/config');

/**
 * Create Glacier instance
 * @param location
 * @returns {Promise<AWS.Glacier>}
 */
const createInstance = async (location) => {
  // Get AWS credentials by location.credentials
  const awsCredentials = await credentialService.getCredentialById(location.credentials);

  // Configure AWS and Return new instance of Glacier
  AWS.config = new AWS.Config({
    accessKeyId: awsCredentials.key,
    secretAccessKey: awsCredentials.secret,
    region: config.aws.region,
  });
  return new AWS.Glacier();
};

/**
 * Check if Glacier vault exists
 * @param glacier
 * @param vaultName
 * @returns {Promise<boolean>}
 */
const vaultExists = async (glacier, vaultName) => {
  try {
    await glacier.describeVault({ accountId: '-', vaultName }).promise();
    return true;
  } catch (e) {
    if (e.statusCode === 404) return false;
    logger.error(` ERROR! --- Access to bucket ${vaultName} is denied! error: ${e.message}`);
    throw new Error(` ERROR! --- Access to bucket ${vaultName} is denied! error: ${e.message}`);
  }
};

/**
 * Create new Glacier vault
 * @param glacier
 * @param vaultName
 * @returns {Promise<void>}
 */
const createNewVault = async (glacier, vaultName) => {
  await glacier.createVault({ vaultName }).promise();
  logger.verbose(` == Vault ${vaultName} created == `);
};

/**
 * Initiate multipart upload
 * @param glacier
 * @param vaultName
 * @returns {Promise<string>}
 */
const initiateMultipartUpload = async (glacier, vaultName) => {
  const data = await glacier
    .initiateMultipartUpload({
      vaultName,
      partSize: `${config.glacier.uploadDataSize}`,
    })
    .promise();
  return data.uploadId;
};

/**
 * Upload multipart part
 * @param glacier
 * @param params
 * @returns {Promise<Request<Glacier.UploadMultipartPartOutput, AWSError>>}
 */
const uploadMultipartPart = async (glacier, params) => glacier.uploadMultipartPart(params).promise();

/**
 * Complete multipart upload
 * @param glacier
 * @param params
 * @returns {*}
 */
const completeMultipartUpload = (glacier, params) => glacier.completeMultipartUpload(params).promise();

module.exports = {
  createInstance,
  vaultExists,
  createNewVault,
  initiateMultipartUpload,
  uploadMultipartPart,
  completeMultipartUpload,
};
