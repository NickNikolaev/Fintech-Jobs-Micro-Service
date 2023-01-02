const stream = require('stream');

// Transform stream that stringifies data
const stringifyTransform = () => {
  return new stream.Transform({
    writableObjectMode: true,

    transform(chunk, encoding, callback) {
      const data = JSON.stringify(chunk);

      // Push the data onto the readable queue.
      callback(null, data);
    },
  });
};

module.exports = stringifyTransform;
