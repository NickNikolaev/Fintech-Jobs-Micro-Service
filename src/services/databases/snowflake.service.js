const snowflake = require('snowflake-sdk');
const genericPool = require('generic-pool');
const logger = require('../../config/logger');
const config = require('../../config/config');
require('dotenv').config();

/**
 * Connect to Snowflake
 * @param connectionDetails
 * @returns {Promise<*>}
 */
const connect = async (connectionDetails) => {
  const { account, password, username, warehouse, database } = connectionDetails;
  const connectionOptions = { schema: 'MAIN' };

  // Create Snowflake connection
  const connection = snowflake.createConnection({
    account,
    username,
    password,
    warehouse,
    database,
    ...connectionOptions,
  });

  // Connect to Snowflake
  await connection.connect();
  logger.verbose(' == Connected to Snowflake == ');
  return connection;
};

const factory = {
  create: () =>
    new Promise((resolve, reject) => {
      const connection = snowflake.createConnection({
        account: config.snowflake.host,
        password: config.snowflake.password,
        username: config.snowflake.user,
        warehouse: config.snowflake.warehouse,
        database: config.snowflake.db,
      });
      connection.connect((err, conn) => {
        if (err) {
          logger.error(`Unable to connect: ${err.message}`);
          reject(new Error(err.message));
        } else {
          logger.info(`Successfully connected to Snowflake, ID: ${conn.getId()}`);
          resolve(conn);
        }
      });
    }),
  destroy: (connection) =>
    new Promise((resolve, reject) => {
      connection.destroy((err, conn) => {
        if (err) {
          logger.error(`Unable to disconnect: ${err.message}`);
        } else {
          logger.info(`Disconnected connection with id: ${conn.getId()}`);
        }
        resolve();
      });
    }),
  validate: (connection) => new Promise((resolve) => resolve(connection.isUp())),
};

const poolOptions = {
  max: 10,
  min: 3,
  testOnBorrow: true,
  acquireTimeoutMillis: 60000,
  evictionRunIntervalMillis: 900000,
  numTestsPerEvictionRun: 4,
  idleTimeoutMillis: 10800000,
};

const snowflakePool = genericPool.createPool(factory, poolOptions);

/**
 * Execute Snowflake query
 * @param query
 * @param bindParams
 * @returns {Promise<unknown>}
 */
const executeQuery = (query, bindParams = []) =>
  new Promise((resolve, reject) =>
    snowflakePool
      .acquire()
      .then((connection) => {
        connection.execute({
          sqlText: query,
          binds: bindParams,
          complete: (err, stmt, rows) => {
            // If error -> Reject
            if (err) {
              logger.error(`Snowflake - Error CODE: ${err.code} \n - Query: ${query} \n - Error: ${err}`);
              reject(err);
            }

            // Resolve rows
            return resolve(rows);
          },
        });
        snowflakePool.release(connection);
      })
      .catch(reject)
  );

module.exports = {
  connect,
  executeQuery,
};
