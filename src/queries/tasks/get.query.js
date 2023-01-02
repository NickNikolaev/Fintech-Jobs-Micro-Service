const generateGetTaskByIdQuery = (taskId) =>
  `SELECT * FROM ECHO_DEV.MILEMARKER.FOXTROT_TASK_GET WHERE "milemarkerSystemId" = ${taskId};`;

module.exports = {
  generateGetTaskByIdQuery,
}
