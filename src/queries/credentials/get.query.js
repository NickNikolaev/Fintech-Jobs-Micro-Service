const generateGetCredentialByIdQuery = (credentialId) =>
  `SELECT * FROM MILEMARKER.FOXTROT_CREDENTIAL_GET WHERE "milemarkerSystemId" = ${credentialId};`;

module.exports = {
  generateGetCredentialByIdQuery,
}