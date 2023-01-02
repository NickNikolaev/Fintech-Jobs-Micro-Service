const executeAllPromisesInParallel = async (data, functionToExecute) => {
  // Get all data promises (resolved and rejected)
  const dataPromises = await Promise.allSettled(
    data.reduce((promises, dataObject) => {
      // Create promise
      const promise = new Promise((resolve, reject) => functionToExecute(dataObject, resolve, reject));

      // Push promise to "promises" and Return them
      promises.push(promise);
      return promises;
    }, [])
  );

  // Get all data errors
  const dataErrors = dataPromises
    .filter((dataPromise) => dataPromise.status === 'rejected')
    .map((dataPromise) => dataPromise.reason);

  // If there are data errors -> Throw them
  if (dataErrors.length > 0) throw dataErrors;
};

module.exports = executeAllPromisesInParallel;
