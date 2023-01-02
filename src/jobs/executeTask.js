const {
  moveController,
  parseController,
  apiController,
  convertFileController,
  createFileController,
  transferFileController,
  mojoController,
  getDataController,
  postDataController,
  updateJobController,
} = require('../controllers');

const executeTask = async (context) => {
  switch (context.task.type) {
    case 'move':
      await moveController(context);
      break;

    case 'parse':
      await parseController(context);
      break;

    case 'api':
      await apiController(context);
      break;

    case 'convertFile':
      await convertFileController(context);
      break;

    case 'createFile':
      await createFileController(context);
      break;

    case 'transferFile':
      await transferFileController();
      break;

    case 'mojo':
      await mojoController(context);
      break;

    case 'getData':
      await getDataController(context);
      break;

    case 'postData':
      await postDataController(context);
      break;

    case 'updateJob':
      await updateJobController(context);
      break;

    default:
      break;
  }
};

module.exports = executeTask;
