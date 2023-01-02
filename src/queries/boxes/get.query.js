const generateGetBoxByIdQuery = (boxId) =>
  `SELECT * FROM MILEMARKER.FOXTROT_BOX_GET WHERE "milemarkerSystemId" = ${boxId};`;

module.exports = {
  generateGetBoxByIdQuery,
}
