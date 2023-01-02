const getDBConfigColumns = (dbObject) => {
  // If databases object has columns -> Map them
  if (dbObject.COLUMNSJSON) return dbObject.COLUMNSJSON.map((column) => column.columnName);

  // If databases object has parameters -> Map them
  if (dbObject.PARAMETERSJSON) return dbObject.PARAMETERSJSON.map((parameter) => parameter.parameterName.slice(1));

  // If databases object doesn't have columns and parameters -> Throw error
  throw new Error('Column or Parameters field must be present in dbConfig!!');
};

module.exports = getDBConfigColumns;
