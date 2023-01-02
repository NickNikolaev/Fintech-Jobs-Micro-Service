const { existsSync, createWriteStream } = require('fs');
const mongoose = require('mongoose');
const mkdirp = require('mkdirp');
const ExcelJS = require('exceljs');
const XLSX = require('xlsx');
const logger = require('../config/logger');
const config = require('../config/config');

const XLSX_COLUMN_LIMIT = 16384;
const XLS_COLUMN_LIMIT = 256;

module.exports = class ErrorRecorder {
  constructor(errorAdditionalInfo, format, config) {
    this.format = format;
    this.filePath = `${config.filesFolder}/${errorAdditionalInfo.jobId}/errors/${errorAdditionalInfo.taskName}/`;
    this.config = config;
    this.encoding = this.config.encoding ? this.config.encoding : 'utf8';
    this.jobId = errorAdditionalInfo.jobId;
    this.taskId = errorAdditionalInfo.taskId;
    this.fileId = mongoose.Types.ObjectId();

    switch (this.format) {
      case '.txt':
        this.addRowToErrorFile = async (data, errorText) => {
          this._addRowToCSV(data, errorText);
        };
        this.closeWriteStream = this._closeStreamCSV;
        break;

      case '.csv':
        this.addRowToErrorFile = async (data, errorText) => {
          this._addRowToCSV(data, errorText);
        };
        this.closeWriteStream = this._closeStreamCSV;
        break;

      case '.xls':
        this.addRowToErrorFile = async (data, errorText) => {
          this._addRowToXLS(data, errorText);
        };
        this.closeWriteStream = this._closeStreamXLS;
        break;

      case '.xlsx':
        this.addRowToErrorFile = async (data, errorText) => {
          this._addRowToXLSX(data, errorText);
        };
        this.closeWriteStream = this._closeStreamXLSX;
        break;

      case '.json':
      default:
        this.format = '.json';
        this.addRowToErrorFile = async (data, errorText) => {
          this._addRowToJSON(data, errorText);
        };
        this.closeWriteStream = this._closeStreamJSON;
        break;
    }

    this.fileName = `${errorAdditionalInfo.taskName.replace(/\s+/g, '')}_${errorAdditionalInfo.file}_${
      errorAdditionalInfo.jobStartTime
    }${this.format}`;
  }

  _createFolderPath() {
    mkdirp.sync(this.filePath);
    logger.verbose(` == Create directory path: ${this.filePath} == `);
  }

  _addRowToJSON(data, errorText) {
    if (!existsSync(this.filePath)) this._createFolderPath();

    data.errorText = errorText;

    if (!this.output) {
      this.output = createWriteStream(this.filePath + this.fileName, { encoding: this.encoding });
      this.output.write('[');
      this.output.write(JSON.stringify(data));
    } else {
      this.output.write(`,${JSON.stringify(data)}`);
    }
  }

  _closeStreamJSON() {
    if (this.output) this.output.end(']');
  }

  _addRowToCSV(data, errorText) {
    if (!existsSync(this.filePath)) this._createFolderPath();

    data.errorText = errorText;

    const delimiter = this.config.delimiter ? this.config.delimiter : ',';
    const newline = this.config.newLine ? this.config.newLine : '\n';
    const header = !!this.config.header;
    let quote = false;
    let inputString = '';

    if (this.config.quotes) {
      quote = this.config.quoteChar ? this.config.quoteChar : '"';
    }

    if (!this.output) {
      this.output = createWriteStream(this.filePath + this.fileName, { encoding: this.encoding });

      if (header) {
        let headersString = '';

        headersString += Object.keys(data).map((header) => {
          return quote ? quote + header + quote : header;
        });

        this.output.write(headersString);
        inputString += newline;
      }
    } else {
      inputString += newline;
    }

    for (const cell in data) {
      if (inputString !== newline) inputString += delimiter;

      inputString += quote ? quote + data[cell] + quote : data[cell];
    }

    this.output.write(inputString);
  }

  _closeStreamCSV() {
    if (this.output) this.output.end();
  }

  _addRowToXLSX(data, errorText) {
    if (!existsSync(this.filePath)) this._createFolderPath();

    const header = !!this.config.header;

    // make sure number of columns does not exceed maximum after adding error message
    if (Object.keys(data).length < XLSX_COLUMN_LIMIT) {
      data.errorText = errorText;
    }

    if (!this.output) {
      this.output = createWriteStream(this.filePath + this.fileName, { encoding: this.encoding });

      this.workbook = new ExcelJS.stream.xlsx.WorkbookWriter({
        stream: this.output,
        useStyles: false,
        useSharedStrings: false,
      });
      this.workbookCommitted = false;
      this.worksheet = this.workbook.addWorksheet('Errors');

      if (header) {
        this.worksheet.addRow(Object.keys(data)).commit();
      }
    }

    !this.worksheet.committed && this.worksheet.addRow(Object.values(data)).commit();
  }

  async _closeStreamXLSX() {
    if (this.worksheet && !this.worksheet.committed) await this.worksheet.commit();

    if (!this.workbookCommitted && this.workbook) {
      this.workbookCommitted = true;
      this.workbook.commit();
    }
  }

  // TODO: streams (now is taking way to much memory)
  _addRowToXLS(data, errorText) {
    if (!existsSync(this.filePath)) this._createFolderPath();

    // make sure number of columns does not exceed maximum after adding error message
    if (Object.keys(data).length < XLS_COLUMN_LIMIT) {
      data.errorText = errorText;
    }

    if (!this.wbXls) {
      this.wbXls = XLSX.utils.book_new();
      this.allErrorData = [];
    }

    this.allErrorData.push(data);
  }

  async _closeStreamXLS() {
    const header = !!this.config.header;
    if (this.allErrorData) {
      const wsXls = await XLSX.utils.json_to_sheet(this.allErrorData, {
        skipHeader: !header,
      });

      XLSX.utils.book_append_sheet(this.wbXls, wsXls, 'errors');
      await XLSX.writeFile(this.wbXls, this.filePath + this.fileName, { bookType: 'xls' });
    }
  }
};
