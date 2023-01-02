const generateGetConnectionByIdQuery = (connectionId) =>
  `SELECT * FROM MILEMARKER.FOXTROT_CONNECTION_GET WHERE "milemarkerSystemId" = ${connectionId};`;

module.exports = {
  generateGetConnectionByIdQuery,
}
