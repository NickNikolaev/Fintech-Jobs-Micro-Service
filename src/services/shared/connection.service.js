const { generateGetConnectionByIdQuery } = require('../../queries/connections/get.query');
const { snowflakeService } = require('../databases');

/**
 * Get connection by id
 * @param connectionId
 * @returns {Promise<*>}
 */
const getConnectionById = async (connectionId) => {
  // Generate get connection by id query and Execute it
  const getConnectionByIdQuery = generateGetConnectionByIdQuery(connectionId);
  const connection = await snowflakeService.executeQuery(getConnectionByIdQuery);

  // If connection is not found -> Throw error
  if (connection.length === 0) throw new Error(`Connection with id ${connectionId} was not found!`);

  // Return connection
  return connection[0];
};

module.exports = {
  getConnectionById,
};
