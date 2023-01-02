const logger = require('../config/logger');
const executeAllJobTasks = require('./executeAllJobTasks');
const deleteGetDataTasksCache = require('./deleteGetDataTasksCache');

const executeJob = async (job) => {
  // Job report saved in cloud and local databases
  const jobReport = {
    jobId: job.attrs._id,
    jobStartTime: job.attrs.lockedAt,
    status: 'inProgress',
    tasks: [],
  };
  logger.info(`Job ${job.attrs._id} started`, jobReport);

  // Execute all job's tasks
  const context = { job, jobReport };
  await executeAllJobTasks(context);

  // If there is failed task -> Throw error
  const failedTask = jobReport.tasks.find((task) => task.status === 'failed');
  if (failedTask) {
    jobReport.status = 'failed';
    logger.info(`Job ${job.attrs._id} is done`, jobReport);
    throw new Error(`Job ${job.attrs._id} failed!`);
  }

  // Delete "getData" tasks' cache
  await deleteGetDataTasksCache(job);

  // Successful job
  jobReport.status = 'success';
  logger.info(` == JOB ${job.attrs._id} done successfully == `, jobReport);
};

module.exports = executeJob;
