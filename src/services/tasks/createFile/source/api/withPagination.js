const createFileFromHubspotPagination = require('./shared/createFileFromHubspotPagination');
const createFileFromSalesforcePagination = require('./shared/createFileFromSalesforcePagination');

const withPagination = async (context) => {
  switch (context.task.config.pagination.type) {
    case 'hubspot': {
      await createFileFromHubspotPagination(context);
      break;
    }

    case 'salesforce': {
      await createFileFromSalesforcePagination(context);
      break;
    }

    default:
      break;
  }
};

module.exports = withPagination;
