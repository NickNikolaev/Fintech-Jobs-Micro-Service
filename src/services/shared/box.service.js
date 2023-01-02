const { generateGetBoxByIdQuery } = require('../../queries/boxes/get.query');
const { snowflakeService } = require('../databases');

/**
 * Get box by id
 * @param boxId
 * @returns {Promise<*>}
 */
const getBoxById = async (boxId) => {
  // Generate get box by id query and Execute it
  const getBoxByIdQuery = generateGetBoxByIdQuery(boxId);
  const box = await snowflakeService.executeQuery(getBoxByIdQuery);

  // If box is not found -> Throw error
  if (box.length === 0) throw new Error(`Box with id ${boxId} was not found!`);

  // Return box
  return box[0];
};

module.exports = {
  getBoxById,
};
