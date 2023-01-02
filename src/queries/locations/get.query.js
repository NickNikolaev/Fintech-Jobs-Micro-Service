const generateGetLocationByIdQuery = (locationId) =>
  `SELECT *
   FROM MILEMARKER.FOXTROT_LOCATION_GET
   WHERE "milemarkerSystemId" = ${locationId};`;

module.exports = {
  generateGetLocationByIdQuery,
};
