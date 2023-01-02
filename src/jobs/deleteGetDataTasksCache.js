const { taskService, cacheService } = require('../services/shared');

const deleteGetDataTasksCache = (job) =>
  Promise.all(
    job.attrs.data.tasks.map(async (taskId) => {
      // Get task by id
      const { type, config, milemarkerSystemId } = await taskService.getTaskById(taskId);

      // If task's type is "getData" and config.deleteCache = true -> Delete task id from Redis
      if (type === 'getData' && config.deleteCache) await cacheService.del(milemarkerSystemId);
    })
  );

module.exports = deleteGetDataTasksCache;
