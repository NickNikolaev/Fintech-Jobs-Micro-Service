const { snowflakeService } = require('../databases');
const {
  generateGetDbObjectByIdQuery,
  generateGetDbObjectBySchemaAndNameQuery,
} = require('../../queries/dbObjects/get.query');

/**
 * Get databases object by id
 * @param dbObjectId
 * @returns {Promise<*>}
 */
const getDbObjectById = async (dbObjectId) => {
  // Generate get databases object by id query and Execute it
  const getDbObjectByIdQuery = generateGetDbObjectByIdQuery(dbObjectId);
  const dbObject = await snowflakeService.executeQuery(getDbObjectByIdQuery);

  // If dbObject is not found -> Throw error
  if (dbObject.length === 0) throw new Error(`DB Object with id ${dbObjectId} was not found!`);

  // Return dbObject
  return dbObject[0];
};

/**
 * Get databases object by object schema and object name
 * @param dbObjectString
 * @returns {Promise<*>}
 */
const getDbObjectBySchemaAndName = async (dbObjectString) => {
  const [schema, name] = dbObjectString.split('.');

  // Generate get databases object by schema and name query and Execute it
  const getDbObjectBySchemaAndNameQuery = generateGetDbObjectBySchemaAndNameQuery(schema, name);
  const dbObject = await snowflakeService.executeQuery(getDbObjectBySchemaAndNameQuery);

  // If dbObject is not found -> Throw error
  if (dbObject.length === 0) throw new Error(`DB Object with schema ${schema} and name ${name} was not found!`);

  // Return dbObject
  return dbObject[0];
};

module.exports = {
  getDbObjectById,
  getDbObjectBySchemaAndName,
};
