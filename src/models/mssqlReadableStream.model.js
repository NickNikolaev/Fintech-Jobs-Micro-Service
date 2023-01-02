const stream = require('stream');
const mapData = require('../services/tasks/createFile/shared/mapData');
const logger = require('../config/logger');

// Wrapper around node-mssql request to make it readable stream that can be piped
module.exports = class MssqlReadableStream extends stream.Readable {
  constructor(request, task, endReportFile, options) {
    super(options);
    this.request = request;
    this.task = task;
    this.lines = 0;
    this.endReportFile = endReportFile;
    this.endReportFile.linesProcessed = 0;

    this._processRow = this._processRow.bind(this);
    this._endStream = this._endStream.bind(this);
    this._handleError = this._handleError.bind(this);

    this.request.on('row', this._processRow);
    this.request.on('done', this._endStream);
    this.request.on('error', this._handleError);

    this.request.pause();
  }

  _endStream() {
    // push end of the stream
    if (this.lines) this.push(']');
    this.push(null);
  }

  _handleError(error) {
    logger.error(error);
    this.destroy(error);
  }

  _processRow(row) {
    if (this.lines === 0) this.push('[');
    if (this.lines > 0) this.push(',');

    this.endReportFile.linesProcessed++;
    this.lines++;
    logger.debug(`Lines done: ${this.endReportFile.linesProcessed}`);

    if (!this.push(JSON.stringify(mapData({}, this.task, row)))) {
      this.request.pause();
    }
  }

  _read(size) {
    this.request.resume();
  }
};
