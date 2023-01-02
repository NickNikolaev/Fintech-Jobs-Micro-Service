const appendCSV = require('./appendCSV');
const appendJSON = require('./appendJSON');

const appendDataToFiles = async (task, errorAdditionalInfo, fileContexts, jsonData) => {
  // Generate append data to file promises
  const appendDataToFilePromises = task.file.format.map(async (format) => {
    // Find file context with the current file format
    const fileContext = fileContexts.find(({ file }) => file.type === format);

    // Append data to the appropriate format file
    switch (format) {
      case '.txt':
      case '.csv': {
        await appendCSV(errorAdditionalInfo, fileContext.file, task, jsonData);
        break;
      }

      case '.json': {
        await appendJSON(errorAdditionalInfo, fileContext.file, task, jsonData);
        break;
      }

      default: {
        throw new Error('Not supported format');
      }
    }
  });

  // Resolve all append data to file promises
  const response = await Promise.allSettled(appendDataToFilePromises);

  // Get all rejected promises
  const rejectedPromises = response
    .filter((promise) => promise.status === 'rejected')
    .map((promise) => ({ message: promise.reason }));

  // If there are rejected promises -> Map them to string and Throw them as error
  if (rejectedPromises.length > 0) throw rejectedPromises;
};

module.exports = appendDataToFiles;
