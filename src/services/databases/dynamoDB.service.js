const AWS = require('aws-sdk');
const logger = require('../../config/logger');

const checkTable = async (dynamodb, tableName) => {
  try {
    await dynamodb.describeTable({ TableName: tableName }).promise();
  } catch (e) {
    if (e.statusCode === 400) {
      throw new Error(` = There is no table '${tableName}' in dynamoDB =`);
      // await createTable(dynamodb, tableName)
    } else {
      throw new Error(' = Failed to connect to dynamoBD =');
    }
  }
};

/**
 * Connect to DynamoDB
 * @param credentials
 * @param location
 * @returns {Promise<AWS.DynamoDB.DocumentClient>}
 */
const connect = async (credentials, location) => {
  if (!location.table) throw Error(' ERROR!! -- location should contain table name when connection to dynamoDB!');

  AWS.config = new AWS.Config({
    accessKeyId: credentials.key,
    secretAccessKey: credentials.secret,
    region: location.region,
  });

  const dynamodb = await new AWS.DynamoDB();

  // Check if table exists
  await checkTable(dynamodb, location.table);

  const docClient = new AWS.DynamoDB.DocumentClient();

  logger.verbose(' = Connected to dynamoDB = ');

  return docClient;
};

/**
 * Save data in DynamoDB
 * @param dbConfig
 * @param data
 * @param fileReport
 * @param errorRecorder
 * @returns {Promise<void>}
 */
const saveData = async (dbConfig, data, fileReport, errorRecorder) => {
  const { db, tableName } = dbConfig;

  for (let i = 0; i < data.length; i++) {
    fileReport.linesProcessed++;
    const params = {
      TableName: tableName,
      Item: data[i],
      ReturnValues: 'ALL_OLD',
    };
    try {
      const previousData = await db.put(params).promise();
      fileReport.linesSucceeded++;
      if (previousData.Attributes) fileReport.linesUpdated++;
    } catch (e) {
      if (!fileReport.errorFilePath) fileReport.errorFilePath = errorRecorder.filePath + errorRecorder.fileName;
      await errorRecorder.addRowToErrorFile(data[i], e.message);
      fileReport.linesErrored++;
      throw new Error(` = Upload line to dynamodb failed. error: ${e.message} =`);
    } finally {
      logger.debug(` = Lines done: ${fileReport.linesProcessed}, erroredLines: ${fileReport.linesErrored} = `);
    }
  }
};

module.exports = {
  connect,
  saveData,
};
