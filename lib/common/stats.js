var async = require('async'),
  config = require('../../config'),
  prettyBytes = require('pretty-bytes');

var Stats = function (redisClient, type, data) {
  data = data || {};

  this.type = type;
  this.start = Date.now();
  this.total = data.total || 0;
  this.processed = data.processed || 0;
  this.processedOffset = data.processedOffset || 0;
  this.dataOffset = data.dataOffset || 0;
  this.totalData = data.totalData || 0;
  this.perWindow = [];
  this.client = redisClient;
};

Stats.prototype.currentProcessed = function() {
  return this.processed - this.processedOffset;
};

Stats.prototype.currentData = function () {
  return this.totalData - this.dataOffset;
};

Stats.prototype.dataPerSecond = function () {
  return (this.currentData() / ((Date.now() - this.start) / 1000)).toPrecision(4) * 1;
};

Stats.prototype.percentComplete = function () {
  return ((this.processed / this.total) * 100).toPrecision(4) * 1.0;
};

Stats.prototype.getStats = function () {
  return {
    total: this.total,
    remaining: this.total - this.processed,
    processed: this.currentProcessed(),
    totalProcessed: this.processed,
    data: this.currentData(),
    totalData: this.totalData,
    dataPerSecond: prettyBytes(this.dataPerSecond()),
    perSecond: this.perSecond,
    perSecondAverage: (this.perWindow.reduce(function (a, b) {
      return a + b
    }) / this.perWindow.length).toPrecision(4) * 1.0,
    percentComplete: this.percentComplete()
  };
};

Stats.prototype.init = function(callback) {
  var self = this;

  self.load(function(err) {
    if (err) {
      callback(err);
      return;
    }

    self.processedOffset = self.processed;
    self.dataOffset = self.totalData;

    callback();
  });
};

Stats.prototype.load = function (callback) {
  var self = this;

  async.parallel([
    function (next) {
      self.client.get(config['total' + self.type + 'Key'], function (err, value) {
        if (err) {
          next(err);
          return;
        }

        self.total = parseInt(value);
        next();
      });
    },
    function (next) {
      self.client.get(config['total' + self.type + 'ProcessedKey'], function (err, value) {
        if (err) {
          next(err);
          return;
        }

        var now = Date.now(),
          countDelta = parseInt(value) - self.processed;

        if (self.last) {
          self.perSecond = (countDelta / ((now - self.last) / 1000)).toPrecision(4) * 1;
          self.perWindow.push(self.perSecond);
          self.perWindow = self.perWindow.slice(0, 40); // average for windows
        }

        self.processed = parseInt(value);
        self.last = now;

        next();
      });
    },
    function (next) {
      self.client.get(config['total' + self.type + 'SizeKey'], function (err, value) {
        if (err) {
          next(err);
          return;
        }

        self.totalData = parseInt(value);
        next();
      });
    },
  ], callback);
};


exports.Stats = Stats;

