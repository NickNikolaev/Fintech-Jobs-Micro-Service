const { snowflakeService } = require('../databases');
const { generateGetTaskByIdQuery } = require('../../queries/tasks/get.query');
const { generateUpdateTaskAlertByIdQuery } = require('../../queries/tasks/put.query');

/**
 * Get task by id
 * @param taskId
 * @returns {Promise<*>}
 */
const getTaskById = async (taskId) => {
  // Generate get task by id query and Execute it
  const getTaskByIdQuery = generateGetTaskByIdQuery(taskId);
  const task = await snowflakeService.executeQuery(getTaskByIdQuery);

  // If task is not found -> Throw error
  if (task.length === 0) throw new Error(`Task with id ${taskId} was not found!`);

  // Return task
  return task[0];
};

/**
 * Update task alert by id
 * @param taskId
 * @param alert
 * @param alertCount
 * @param alertMetaData
 * @param urgentLevel
 * @param username
 * @returns {Promise | Promise<unknown>}
 */
const updateTaskAlertById = (taskId, alert, alertCount, alertMetaData, urgentLevel, username) => {
  // Generate update task alert by id query and Execute it
  const updateTaskAlertByIdQuery = generateUpdateTaskAlertByIdQuery(
    taskId,
    alert,
    alertCount,
    alertMetaData,
    urgentLevel,
    username || 'milemarker'
  );
  return snowflakeService.executeQuery(updateTaskAlertByIdQuery);
};

module.exports = {
  getTaskById,
  updateTaskAlertById,
};
