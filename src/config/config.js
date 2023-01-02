const dotenv = require('dotenv');
const path = require('path');
const Joi = require('joi');

dotenv.config({ path: path.join(__dirname, '../../.env') });

const envVarsSchema = Joi.object()
  .keys({
    NODE_ENV: Joi.string().valid('production', 'development', 'test').required(),
    BOX_ID: Joi.number().required(),
    FILES_FOLDER: Joi.string().required(),

    MONGO_LOCAL_URI: Joi.string().required(),
    MONGO_CLOUD_URI: Joi.string().required(),

    AWS_REGION: Joi.string().required(),
    AWS_MAX_ERROR_LIMIT: Joi.number().required(),

    GLACIER_UPLOAD_DATA_SIZE: Joi.number().required(),
    GLACIER_MAX_CONNECTIONS: Joi.number().required(),

    SNOWFLAKE_MASTER_DB: Joi.string().required(),
    SNOWFLAKE_MASTER_DB_SCHEMA: Joi.string().required(),
    SNOWFLAKE_MASTER_WAREHOUSE: Joi.string().required(),
    SNOWFLAKE_MASTER_USER: Joi.string().required(),
    SNOWFLAKE_MASTER_PASSWORD: Joi.string().required(),
    SNOWFLAKE_MASTER_HOST: Joi.string().required(),
    SNOWFLAKE_MASTER_LOG_TABLE: Joi.string().required(),
    SNOWFLAKE_MASTER_API_HOST: Joi.string().required(),
    SNOWFLAKE_MASTER_API_TOKEN: Joi.string().required(),

    REDIS_PORT: Joi.number().required(),
    REDIS_HOST: Joi.string().required(),
    REDIS_FOLDER: Joi.string().required(),
  })
  .unknown();

const { value: envVars, error } = envVarsSchema.prefs({ errors: { label: 'key' } }).validate(process.env);

if (error) throw new Error(`Config validation error: ${error.message}`);

module.exports = {
  env: envVars.NODE_ENV,
  boxId: envVars.BOX_ID,
  filesFolder: envVars.FILES_FOLDER,
  mongo: {
    localURI: envVars.MONGO_LOCAL_URI,
    cloudURI: envVars.MONGO_CLOUD_URI,
  },
  aws: {
    region: envVars.AWS_REGION,
    maxErrorLimit: envVars.AWS_MAX_ERROR_LIMIT,
  },
  glacier: {
    uploadDataSize: envVars.GLACIER_UPLOAD_DATA_SIZE,
    maxConnections: envVars.GLACIER_MAX_CONNECTIONS,
  },
  snowflake: {
    db: envVars.SNOWFLAKE_MASTER_DB,
    dbSchema: envVars.SNOWFLAKE_MASTER_DB_SCHEMA,
    warehouse: envVars.SNOWFLAKE_MASTER_WAREHOUSE,
    user: envVars.SNOWFLAKE_MASTER_USER,
    password: envVars.SNOWFLAKE_MASTER_PASSWORD,
    host: envVars.SNOWFLAKE_MASTER_HOST,
    logTable: envVars.SNOWFLAKE_MASTER_LOG_TABLE,
    apiToken: envVars.SNOWFLAKE_MASTER_API_TOKEN,
  },
  redis: {
    port: envVars.REDIS_PORT,
    host: envVars.REDIS_HOST,
    folder: envVars.REDIS_FOLDER,
  },
};
