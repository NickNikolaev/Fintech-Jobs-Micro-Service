const Client = require('ssh2-sftp-client');
const logger = require('../../config/logger');

/**
 * Connect to SFTP
 * @param context
 * @returns {*}
 */
const connect = async (context) => {
  const { username, password } = context.credentials;
  const { host, port } = context.location;

  // Create new SFTP Client
  const sftp = new Client();
  const config = {
    host,
    port,
    username,
    password,
  };

  // Connect to SFTP and Return the client
  await sftp.connect(config);
  logger.verbose(' == Connected to SFTP == ');
  return sftp;
};

module.exports = {
  connect,
};
