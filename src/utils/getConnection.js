const logger = require('../config/logger');
const { snowflakeService, mssqlService, knexService, dynamoDBService } = require('../services/databases');
const {
  locationService,
  credentialService,
  dbObjectService,
  connectionService,
  cacheService,
} = require('../services/shared');
const interpolateObject = require('./interpolateObject');

// useNodeMssql is to use mssql library instead knex, because knex has problems with mssql streams

/**
 * Get connection
 * @param context
 * @param locationId    location's id
 * @param useNodeMssql  if true -> use mssql library instead of knex
 * @return {Promise}    resolves to database configuration
 */
const getConnection = async (context, locationId, useNodeMssql = false) => {
  const { task } = context;
  let connection;
  let db;
  let dbObject;
  let query;
  let credentials;

  // Get location by id
  const location = await locationService.getLocationById(locationId);

  // If location has credentials -> Get credentials by id and Interpolate them
  if (location.credentials > 0) {
    // Get credentials by id
    credentials = await credentialService.getCredentialById(location.credentials);

    // If task.location = { source, cache } -> Interpolate credentials
    if (task.location.source && task.location.cache) {
      // Get cache by task.location.cache
      const cache = await cacheService.get(task.location.cache);

      // Get every interpolation object, whose type is "credentials"
      const credentialsInterpolationObjects = task.config.map.filter((mapObject) => mapObject.type === 'credentials');

      // Interpolate credentials
      credentials = interpolateObject(credentialsInterpolationObjects, cache[0], credentials);
    }
  }
  const { pagination, type, endpoint, method, dataObject, timeout, table, host, port, user, password, database, config } =
    location;

  switch (type) {
    case 's3':
      return { credentials, dbName: 'S3' };

    case 'api': {
      // If (location.credentialsType = 'Allianz') return { dbName: 'api', endpoint, method, dataObject, timeout, credentialToken: 'Bearer ' }
      // If location data doesn't have credentials -> Return database connection without API credentials
      if (location.credentials === 0)
        return {
          dbName: 'api',
          endpoint,
          method,
          dataObject,
          pagination,
          timeout,
          config,
        };

      logger.debug('before getting credentials token');
      const credentialToken = await credentialService.getCredentialTokenByAuthentication(credentials.authentication);
      logger.debug('after getting credentials token', credentialToken);
      return {
        pagination,
        dbName: 'api',
        endpoint,
        method,
        dataObject,
        authentication: credentials.authentication,
        timeout,
        config,
        credentialToken,
      };
    }

    case 'glacier':
      return { dbName: 'glacier', credentials };

    case 'dbConnection':
      if (location.object.id) dbObject = await dbObjectService.getDbObjectById(location.object.id);
      if (location.object.dbObject) dbObject = await dbObjectService.getDbObjectBySchemaAndName(location.object.dbObject);
      if (location.object.query) query = location.object.query;

      connection = location.object.id
        ? await connectionService.getConnectionById(dbObject.connection)
        : location.object.connectionDetails;

      // If connection's database type is "snowflake" -> Connect to snowflake
      if (connection.databaseType === 'snowflake') db = await snowflakeService.connect(connection);

      // If connection's database type is "mssql" and "useNodeMssql" is true -> Connect to mssql
      if (connection.databaseType === 'mssql' && useNodeMssql) db = await mssqlService.connect(connection);

      // If connection's database is not snowflake and Not using mssql library -> Connect to knex
      if (connection.databaseType !== 'snowflake' && !useNodeMssql) db = await knexService.connect(connection);

      // Return databases connection
      return { dbName: 'direct connection', dbObject, connection, db, query };

    case 'dynamo':
      db = await dynamoDBService.connect(credentials, location);
      return { dbName: 'dynamoDB', tableName: table, db };

    case 'redshift':
      connection = {
        databaseType: 'redshift',
        databaseHost: host,
        databasePort: Number(port),
        databaseUsername: user,
        databasePassword: password,
        databaseName: database,
      };
      db = await knexService.connect(connection);
      return { dbName: 'redshift', tableName: table, db };

    default:
      break;
  }
};

module.exports = getConnection;
