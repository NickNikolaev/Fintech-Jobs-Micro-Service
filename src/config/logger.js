const winston = require('winston');
const config = require('./config');

const enumerateErrorFormat = winston.format((info) => {
  if (info instanceof Error) Object.assign(info, { message: info.stack });

  return info;
});

const transports = {
  info: new winston.transports.Console({
    level: 'debug',
    format: winston.format.combine(winston.format.colorize(), winston.format.simple()),
    silent: process.env.NODE_ENV === 'test',
  }),
  error: new winston.transports.File({
    level: 'error',
    filename: 'src/logs/errors.log',
    format: winston.format.combine(winston.format.timestamp(), winston.format.json()),
    json: true,
    silent: process.env.NODE_ENV === 'test',
  }),
};

const logger = winston.createLogger({
  level: config.env === 'development' ? 'debug' : 'info',
  format: winston.format.combine(
    enumerateErrorFormat(),
    config.env === 'development' ? winston.format.colorize() : winston.format.uncolorize(),
    winston.format.splat(),
    winston.format.printf(({ level, message }) => `${level}: ${message}`)
  ),
  transports: [transports.info, transports.error],
  exitOnError: false,
  handleExceptions: true,
});

module.exports = logger;
