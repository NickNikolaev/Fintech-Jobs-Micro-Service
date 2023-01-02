const logger = require('./config/logger');
const config = require('./config/config');
const executeJob = require('./jobs');
const { setupAgenda, getAgendaInstance } = require('./config/agenda');

setupAgenda('agendaJobs');
const agenda = getAgendaInstance();

// Handle warning, uncaught exception
process.on('warning', (error) => logger.warn(error.stack));
process.on('uncaughtException', (error) => logger.error(error.stack));

// If this module was run directly from the command line -> Start agenda
if (require.main === module)
  agenda
    .start()
    .then(() => {
      // Execute jobs for box id
      agenda.define(config.boxId, { lockLifetime: 40 * 60 * 1000 }, executeJob);

      // Log agenda error
      agenda.on(`fail:${config.boxId}`, (err, job) => logger.error(`Job ${job.attrs._id} failed with error: ${err}`));

      logger.verbose(' === STARTED === ');
    })
    .catch((err) => {
      logger.error('Error: ', err);
      process.exit(1);
    });
