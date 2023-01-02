const logger = require('../config/logger');

/*
 * dbType
 * procedure name - ${schemaName}.${objectName}
 * parameters field from dbobject
 * values data 1 row of data not an array
 * */
const getProcedureQuery = (dbType, procedureName, parameters, values) => {
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
        })
        .join(',');
    } else if (dbType === 'pg' || dbType === 'oracledb') {
      //  for postgresql
      paramPlaceholders = parameters.map(({ ordinalPosition }) => {
        return `$${ordinalPosition}`;
      });
    }

    return `EXEC ${procedureName} ${paramPlaceholders}`;
  }
};

const generateFromQuery = (task, dbConfig, lineLimit) => {
  // If dbConfig.query -> Return it
  logger.debug('inside get from query and logging databases config', dbConfig);
  if (dbConfig.query) return dbConfig.query;

  const { dbObject, connection } = dbConfig;
  let query = '';
  switch (dbObject.objectType) {
    case 'PROCEDURE': {
      if (!task.config.parameters) {
        throw new Error('For Procedure objectType parameters are required');
      }
      query = getProcedureQuery(
        connection.databaseType,
        `${dbObject.schemaName}.${dbObject.objectName}`,
        dbObject.parameters,
        task.config.parameters
      );
      break;
    }
    case 'VIEW':
    case 'BASE TABLE': {
      query = lineLimit
        ? `select TOP ${lineLimit} *
           from [${dbObject.schemaName}].[${dbObject.objectName}]`
        : `select *
           from [${dbObject.schemaName}].[${dbObject.objectName}]`;
      break;
    }
    default: {
      throw new Error(`ObjectType '${dbObject.objectType}' not supported `);
    }
  }

  if (task.config.where) query += ` where ${task.config.where}`;

  logger.debug(query);
  return query;
};

module.exports = generateFromQuery;
