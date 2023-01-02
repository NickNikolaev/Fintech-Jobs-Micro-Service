const executeResolvedPromisesInSerial = (data, functionToExecute, functionProps) =>
  data.reduce(async (previousPromise, dataObject) => {
    // Resolve previous promise
    await previousPromise;

    // Execute function
    await functionToExecute({ ...functionProps, dataObject });
  }, Promise.resolve());

module.exports = executeResolvedPromisesInSerial;
