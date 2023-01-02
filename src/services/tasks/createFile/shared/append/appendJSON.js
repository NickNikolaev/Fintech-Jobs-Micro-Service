const { promises: fsPromises } = require('fs');
const mapData = require('../mapData');

const appendJSON = (errorAdditionalInfo, fileDetails, task, jsonData) => {
  const mappedJsonData = jsonData.map((item) => {
    return mapData(
      {
        ...errorAdditionalInfo,
        file: fileDetails.fullPatch,
      },
      task,
      item
    );
  });

  const fileData = mappedJsonData.length === 0 ? '' : JSON.stringify(mappedJsonData);
  return fsPromises.appendFile(fileDetails.fullPatch, fileData);
};

module.exports = appendJSON;
