const Knex = require('knex');
const logger = require('../../config/logger');

/**
 * Connect to Knex
 * @param connection
 * @returns {Promise<*>}
 */
const connect = async (connection) => {
  const knex = await Knex({
    client: connection.databaseType === 'redshift' ? 'pg' : connection.databaseType,
    connection: {
      host: connection.databaseHost,
      port: connection.databasePort,
      user: connection.databaseUsername,
      password: connection.databasePassword,
      database: connection.databaseName,
      multipleStatements: true,
      pool: { min: 10, max: 1000 },
    },
  });

  await knex.raw('select 1+1 as result');
  logger.verbose(` = Connected to ${connection.databaseType} db = `);
  return knex;
};

module.exports = {
  connect,
};
