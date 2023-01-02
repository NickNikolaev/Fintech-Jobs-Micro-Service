const sql = require('mssql');
const logger = require('../../config/logger');

/**
 * Connect to MSSQL
 * @param connection
 * @returns {Promise<*>}
 */
const connect = async (connection) => {
  const db = await sql.connect({
    server: connection.databaseHost,
    port: connection.databasePort,
    user: connection.databaseUsername,
    password: connection.databasePassword,
    database: connection.databaseName,
    pool: { min: 10, max: 1000 },
    connectionTimeout: 100000,
    requestTimeout: 300000,
    options: {
      trustedConnection: true,
      encrypt: true,
      enableArithAbort: true,
      trustServerCertificate: true,
    },
  });

  logger.verbose(' == Connected to mssql == ');

  return db;
};

/**
 * Execute storage procedure
 * @param db
 * @param procedureName
 * @param params
 * @param paramPlaceholders
 * @returns {Promise<*>}
 */
const executeStorageProcedure = async (db, procedureName, params, paramPlaceholders) => {
  const resource = db.client.pool.acquire();

  const connection = await resource.promise;
  const con = connection.request();
  con.multiple = true;
  const rows = await con.query(`EXEC ${procedureName} ${paramPlaceholders}`);
  db.client.pool.release(connection);

  if (!rows.recordsets) {
    if (rows.length === 1) return rows[0];

    return rows;
  }
  if (rows.recordsets.length !== 1) return rows.recordsets;

  return rows.recordset;
};

module.exports = {
  connect,
  executeStorageProcedure,
};
