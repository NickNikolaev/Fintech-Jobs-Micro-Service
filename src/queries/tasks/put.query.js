const generateUpdateTaskAlertByIdQuery = (
  taskId,
  alert,
  alertCount,
  alertMetaData,
  urgentLevel,
  username
) => `CALL MILEMARKER.FOXTROT_TASK_PUT_ALERT(
      ${taskId},
      ${alert},
      ${alertCount},
      ${alertMetaData.length === 0 ? `'[]'` : `'${JSON.stringify(alertMetaData)}'`},
      ${urgentLevel},
      '${username}');`;

module.exports = {
  generateUpdateTaskAlertByIdQuery,
};
