#!/usr/bin/env node

var async = require('async'),
  cloudApp = require('./common/cloud-app'),
  dziLoader = require('./common/dzi-loader'),
  idLoader = require('./common/id-loader'),
  logging = require('./common/logging'),
  pkgcloud = require('pkgcloud'),
  redis = require('redis');

var programOpts = cloudApp.loadProgram(process.argv);

var redisClient = redis.createClient(),
  storageClient = pkgcloud.providers.rackspace.storage.createClient(programOpts.cloudOptions),
  log = logging.getLogger(process.env.IMPORT_LOG_LEVEL || 'debug'),
  config = require('../config.json');

var data = {
  skipped: 0
};

var exiting = false;

['SIGHUP', 'SIGTERM', 'SIGINT'].forEach(function (signal) {
  process.on(signal, function () {
    log.info('Exiting...');
    exiting = true;
  });
});

idLoader.getIds(storageClient.download({
  container: 'archive',
  remote: 'total.txt'
}), function(err, ids) {

  data.idsRemaining = ids.length;

  displayStats(function () {
    async.forEachLimit(ids, 15, loadDzi, function (err) {
      if (err) {
        log.error(err);
      }

      displayStats(function() {});
    });
  });
});

function loadDzi(id, callback) {
  if (exiting) {
    log.info('Done!');
    process.exit(0);
  }

  dziLoader.loadDzi(id, function (err, result) {
    data.idsRemaining--;

    if (err && err.noDziFound) {
      data.skipped++;
      log.info('Skipping missing dzi', err);
      callback();
      return;
    } else if (err) {
      log.error('Unable to load dzi', err);
      callback(err);
      return;
    }

    async.parallel([
      function (next) {
        redisClient.sadd(config.blobUrlsKey, result.blobs, function(err, count) {
          if (err) {
            return next(err);
          }
          else if (count != result.blobs.length) {
            return next({ message: 'missingBlobs', count: count, blobs: result.blobs });
          }

          next();
        });
      },
      function (next) {
        var payload = {
          xml: result.dzi,
          id: result.id
        };

        redisClient.sadd(config.dzisKey, JSON.stringify(payload), function(err, count) {
          if (err) {
            return next(err);
          }
          else if (count != 1) {
            return next({ message: 'notSaved', count: count, dzi: payload });
          }

          next();
        });
      }], function (err) {
      if (err) {
        log.error('Unable to prepare data for dzi', err);
        callback(err);
        return;
      }
      callback();
    });
  });
}

function displayStats(callback) {

  async.parallel([
    function (next) {
      redisClient.scard(config.blobUrlsKey, function (err, count) {
        data.blobsToImport = count;
        next(err);
      });
    },
    function (next) {
      redisClient.scard(config.dzisKey, function (err, count) {
        data.dzisToUpload = count;
        next(err);
      });
    },
  ], function (err) {
    if (err) {
      log.error('Unable to display stats', err);
    }
    log.info(data);
    callback();
  });
}

setInterval(function () {
  displayStats(function () {
  });
}, 30000);
