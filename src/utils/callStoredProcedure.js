const logger = require('../config/logger');
const databaseServices = require('../services/databases');

const callStoredProcedure = async (db, dbType, procedureName, parameters, values) => {
  parameters.sort((a, b) => a.ordinalPosition - b.ordinalPosition);
  let error = false;
  // pick variables from params
  const params = parameters.map(({ parameterName }) => {
    // TODO check if this is true in other DBs than MySQL
    // parameters are in form p_name
    let paramName;
    if (dbType === 'mysql') {
      paramName = parameterName.substring(2);
    } else if (dbType === 'mssql') {
      paramName = parameterName.substring(1);
    } else if (dbType === 'pg' || dbType === 'oracledb') {
      paramName = parameterName;
    }
    if (values[paramName] === undefined) {
      error = {
        message: ` = Some of the procedure parameters are missing. Parameter name: ${paramName} =`,
        values,
      };
      logger.error(error);
      throw new Error(error.message);
    }

    if (!error) {
      const value =
        values[paramName] === null || typeof values[paramName] === 'number'
          ? values[paramName]
          : values[paramName].toString();

      return value === null || typeof value === 'number'
        ? { value }
        : value.substring(0, 1) === '@'
        ? { value: value.substring(1), variable: true }
        : { value };
    }
  });

  if (!error) {
    const variables = params.filter(({ variable }) => variable).map(({ value }) => value);

    const variablePlaceholders = variables.map((_) => '@??').join(',');
    let paramPlaceholders;
    if (dbType === 'mysql') {
      paramPlaceholders = params.map(({ variable }) => (variable ? '@??' : '?')).join(',');
    } else if (dbType === 'mssql') {
      paramPlaceholders = params
        .map((param) => {
          // TODO
          return param.value === null
            ? 'null'
            : typeof param.value === 'number'
            ? param.value
            : `'${param.value.replace(/'/g, "''")}'`;
          // return param.value === null ? `null` : `'${param.value.replace(/'/g, "''")}'`;
        })
        .join(',');
    } else if (dbType === 'pg' || dbType === 'oracledb') {
      //  for postgresql
      paramPlaceholders = parameters.map(({ ordinalPosition }) => {
        return `$${ordinalPosition}`;
      });
    }

    return databaseServices[dbType].executeStorageProcedure(
      db,
      procedureName,
      params,
      paramPlaceholders,
      variablePlaceholders,
      variables
    );
  }
};

module.exports = callStoredProcedure;
