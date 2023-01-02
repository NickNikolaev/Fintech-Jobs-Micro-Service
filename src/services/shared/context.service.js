const add = (currentContext, newProperties) => {
  // TODO: Add Joi validations

  Object.assign(currentContext, newProperties);
};

module.exports = {
  add,
};
