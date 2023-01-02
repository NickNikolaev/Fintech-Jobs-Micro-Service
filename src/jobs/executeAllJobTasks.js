const { performance } = require('perf_hooks');
const { taskService, reportService, contextService } = require('../services/shared');
const logger = require('../config/logger');
const executeTask = require('./executeTask');
const AlertError = require('../models/AlertError.model');

const executeAllJobTasks = (context) =>
  context.job.attrs.data.tasks.reduce(async (previousPromise, taskId) => {
    try {
      // Resolve previous promise
      await previousPromise;

      // Get task by id
      const task = await taskService.getTaskById(taskId);

      // Create task report
      const taskReport = reportService.createTaskReport(context.jobReport, {
        taskId,
        taskName: task.name,
        status: 'inProgress',
        files: [],
      });

      // Add task and taskReport to "context"
      contextService.add(context, { task, taskReport });
      logger.info(task, context.jobReport);

      // Set task alert to true
      await taskService.updateTaskAlertById(taskId, true, task.alertCount + 1, task.alertMetaData, 0, 'nick');

      // Execute task
      const timeStart = performance.now();
      await executeTask(context);
      const timeEnd = performance.now();

      // Remove task alerts
      await taskService.updateTaskAlertById(taskId, false, 0, [], 0, 'nick');

      // Successful task
      Object.assign(context.taskReport, {
        status: 'success',
        processTime: timeEnd - timeStart,
      });
      logger.verbose(` === Task '${taskId}' done in ${timeEnd - timeStart} ms === `);
    } catch (error) {
      // Turn "error" to array (if needed)
      const taskErrors = !Array.isArray(error) ? [error] : error;

      // Failed task
      context.taskReport.status = 'failed';
      logger.error(` === Task ${taskId} failed. Task errors: ${taskErrors} === `, {
        taskId,
        jobId: context.job.attrs._id,
      });

      // Map task errors to AlertError and Push them to task.alertMetaData
      const taskAlertErrors = taskErrors.map((taskError) => new AlertError(taskError));
      context.task.alertMetaData.push(...taskAlertErrors);

      // Update task alert meta data
      await taskService.updateTaskAlertById(
        taskId,
        true,
        context.task.alertCount + 1,
        context.task.alertMetaData,
        0,
        'nick'
      );
    }
  }, Promise.resolve());

module.exports = executeAllJobTasks;
