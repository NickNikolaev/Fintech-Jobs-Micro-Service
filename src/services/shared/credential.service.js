const { generateGetCredentialByIdQuery } = require('../../queries/credentials/get.query');
const { snowflakeService } = require('../databases');
const qs = require('qs');
const axios = require('axios');
const _ = require('lodash');
const logger = require('../../config/logger');

/**
 * Get credential by id
 * @param credentialId
 * @returns {Promise<*>}
 */
const getCredentialById = async (credentialId) => {
  // Generate get credential by id query and Execute it
  const getCredentialByIdQuery = generateGetCredentialByIdQuery(credentialId);
  const credential = await snowflakeService.executeQuery(getCredentialByIdQuery);

  // If credential is not found -> Throw error
  if (credential.length === 0) throw new Error(`Credential with id ${credentialId} was not found!`);

  // Return credential
  return credential[0];
};

/**
 * Get credential token by authentication
 * @param authentication  credentials' authentication
 * @return {Promise}      resolves to string, containing token type and access token
 */
const getCredentialTokenByAuthentication = async (authentication) => {
  const { method, endpoint, body, headers, authorization, tokenObject } = authentication;

  // If credentials have endpoint -> Execute call and get credentials token
  if (endpoint) {
    const options = {
      method,
      url: endpoint,
      headers: { ...headers },
    };

    if (body) {
      options.data = body.type === 'application/json' ? body.parameters : qs.stringify(body.parameters);
      options.headers = { 'content-Type': body.type, ...headers };
    }

    // If authentication.authorization exists and is enabled -> modify "options", depending on authorization.type
    if (authorization && authorization.enabled === true) {
      // If authorization's type is "Basic Auth" -> Add username and password to "options.auth"
      if (authorization.type === 'Basic Auth') {
        options.auth = {
          username: authorization.parameters.username,
          password: authorization.parameters.password,
        };
      }

      // If authorization's type is "Bearer Token" -> Add authorization to "authorization.parameters.token"
      if (authorization.type === 'Bearer Token') options.headers.authorization = authorization.parameters.token;

      // If authorization's type is not "Basic Auth" and not "Bearer Token" -> Throw error
      if (authorization.type !== 'Basic Auth' && authorization.type !== 'Bearer Token')
        throw new Error('Authorization enabled but no/invalid type of authorization provided!');
    }

    // Execute endpoint call
    logger.debug('Axios options', options);
    const getHostReq = await axios(options);

    // Determine token type, access token and return them
    const tokenType = `${getHostReq.data.token_type || authorization?.tokenType || 'bearer'}`;
    const accessToken = tokenObject ? _.get(getHostReq.data, tokenObject) : getHostReq.data.access_token;
    return `${tokenType} ${accessToken}`;
  }

  logger.debug('RAW TOKEN');
  if (authorization && authorization.enabled === true) {
    // If authorization's type is "Bearer Token" -> Return
    if (authorization.type === 'Bearer Token') return `${authorization.tokenType || 'bearer'} ${authorization.accessToken}`;

    // Throw error
    throw new Error('Authorization enabled but no/invalid type of authorization provided!');
  }
};

module.exports = {
  getCredentialById,
  getCredentialTokenByAuthentication,
};
