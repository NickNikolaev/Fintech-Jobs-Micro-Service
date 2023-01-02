const { generateGetLocationByIdQuery } = require('../../queries/locations/get.query');
const { snowflakeService } = require('../databases');

/**
 * Get location by id
 * @param locationId
 * @returns {Promise<*>}
 */
const getLocationById = async (locationId) => {
  // Generate get location by id query and Execute it
  const getLocationByIdQuery = generateGetLocationByIdQuery(locationId);
  const location = await snowflakeService.executeQuery(getLocationByIdQuery);

  // If location is not found -> Throw error
  if (location.length === 0) throw new Error(`Location with id ${locationId} was not found!`);

  // Return location
  return location[0];
};

module.exports = {
  getLocationById,
};
