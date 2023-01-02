/**
 * Get task report by task id
 * @param {Array} jobReport
 * @param {Number} taskId
 * @returns {Object}
 */
const getTaskReportById = (jobReport, taskId) => jobReport.tasks.find((task) => task.taskId === taskId);

/**
 * Create task report
 * @param jobReport
 * @param taskReport
 * @returns {unknown}
 */
const createTaskReport = (jobReport, taskReport) => {
  // Create task report and Return it
  jobReport.tasks.push(taskReport);
  return jobReport.tasks.at(-1);
};

/**
 * Create file report
 * @param taskReport
 * @param fileReport
 */
const createFileReport = (taskReport, fileReport) => {
  // TODO: Add Joi validations for the file report

  // Create file report and Return it
  taskReport.files.push(fileReport);
  return taskReport.files.at(-1);
};

module.exports = {
  getTaskReportById,
  createTaskReport,
  createFileReport,
};
