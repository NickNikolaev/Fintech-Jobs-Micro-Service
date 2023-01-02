const micromatch = require('micromatch');
const { readdir } = require('fs/promises');

const readFilteredFilesFromDirectory = async (task, folder) => {
  // Read all files from directory
  const allFiles = await readdir(folder, { withFileTypes: true });

  // Return files, which match task.file.filter
  return allFiles.filter((file) => micromatch.isMatch(file.name, task.file.filter)).map((file) => file.name);
};

module.exports = readFilteredFilesFromDirectory;
