const Agenda = require('agenda');
const config = require('./config');

const connectOptions = {
  useNewUrlParser: true,
  useUnifiedTopology: true,
};

let agenda;
const setupAgenda = (collectionName) => {
  agenda = new Agenda({
    db: {
      address: config.mongo.cloudURI,
      collection: collectionName,
      options: connectOptions,
    },
  });
};

const getAgendaInstance = () => agenda;

module.exports = {
  setupAgenda,
  getAgendaInstance,
};
