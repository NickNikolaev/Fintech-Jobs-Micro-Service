const config = require('../../config/config');

const generateGetDbObjectByIdQuery = (dbObjectId) =>
  `SELECT *
   FROM MILEMARKER.FOXTROT_DBOBJECT_GET
   WHERE "milemarkerSystemId" = '${dbObjectId}';`;

const generateGetDbObjectBySchemaAndNameQuery = (objectSchema, objectName) =>
  `SELECT *
   FROM ${config.snowflake.db}.MILEMARKER.FOXTROT_DBOBJECT
   WHERE OBJECTSCHEMA = '${objectSchema}'
     AND OBJECTNAME = '${objectName}'`;

module.exports = {
  generateGetDbObjectByIdQuery,
  generateGetDbObjectBySchemaAndNameQuery,
};
