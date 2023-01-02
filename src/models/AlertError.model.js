const determineAlertError = (error) => {
  const { response, message } = error;

  // If error.response -> API alert error
  if (response) {
    delete response.request;
    return {
      type: 'api',
      error: JSON.stringify(response).replace(/['"]+/g, '`'),
    };
  }

  // Generic alert error
  return {
    type: 'message',
    error: {
      message: message.replace(/['"]+/g, '`'),
    },
  };
};

class AlertError extends Error {
  constructor(error) {
    super(error);

    // Determine alert error, depending on error data
    const alertError = determineAlertError(error);

    this.type = alertError.type;
    this.error = alertError.error;
    this.time = new Date();

    Error.captureStackTrace(this, this.constructor);
  }
}

module.exports = AlertError;
