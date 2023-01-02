const ftp = require('basic-ftp');
const logger = require('../../config/logger');

/**
 * Connect to FTP
 * @param context
 * @returns {Promise}
 */
const connect = async (context) => {
  const { credentials, location } = context;
  const { username, password, encryption } = credentials;
  const { host, port } = location;

  // Create new FTP client
  const client = new ftp.Client(0);
  client.ftp.verbose = false;

  // Connection configuration
  const config = {
    host,
    port,
    user: username,
    password,
  };

  // If encryption is "ftp over tls if available" -> Try to connect over TLS, if not able to, set insecure encryption
  if (encryption === 'ftp over tls if available') {
    config.secure = true;
    try {
      await client.access(config);
      logger.verbose(' == Connected to FTP == ');
      return client;
    } catch (error) {
      logger.verbose('== FTP Client does not support TLS ==');
      config.secure = false;
    }
  }

  // Set security, depending on encryption
  if (encryption === 'plain ftp (insecure)') config.secure = false;
  if (encryption === 'required ftp over tls') config.secure = true;
  if (encryption === 'explicit - implicit settings for tls') config.secure = 'implicit';

  // Connect to FTP and Return the client
  await client.access(config);
  logger.verbose(' == Connected to FTP == ');
  return client;
};

module.exports = {
  connect,
};
