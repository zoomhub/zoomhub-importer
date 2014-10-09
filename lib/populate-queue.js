#!/usr/bin/env node

var async = require('async'),
  cloudApp = require('./common/cloud-app'),
  dziLoader = require('./common/dzi-loader'),
  idLoader = require('./common/id-loader'),
  logging = require('./common/logging'),
  pkgcloud = require('pkgcloud'),
  redis = require('redis');

var interval,
  redisClient,
  programOpts,
  storageClient,
  log = logging.getLogger(process.env.IMPORT_LOG_LEVEL || 'debug'),
  config = require('../config.json'),
  data = {
    skipped: 0
  },
  exiting = false;

cloudApp.loadProgram(process.argv, function(err, options) {

  if (err) {
    log.error(err);
    process.exit(1);
  }

  programOpts = options;

  storageClient = pkgcloud.providers.rackspace.storage.createClient(programOpts.cloudOptions);

  if (programOpts.redis) {
    redisClient = redis.createClient(programOpts.redis.port, programOpts.redis.host);
    redisClient.auth(process.env.REDIS_PASSWORD, function(err) {
      if (err) {
        process.exit(1);
      }

      run();
    });
  }
  else {
    redisClient = redis.createClient();
    run();
  }

  function run() {
    ['SIGHUP', 'SIGTERM', 'SIGINT'].forEach(function (signal) {
      process.on(signal, function () {
        log.info('Exiting...');
        exiting = true;
      });
    });

    idLoader.getIds(storageClient.download({
      container: 'archive',
      remote: 'total.txt'
    }), function (err, ids) {

      data.idsRemaining = ids.length;

      displayStats(function () {
        async.forEachLimit(ids, 15, loadDzi, function (err) {
          if (err) {
            log.error(err);
          }

          clearInterval(interval);

          displayStats(function () {
            process.exit(1);
          });
        });
      });
    });

    interval = setInterval(function () {
      displayStats(function () {
      });
    }, 30000);
  }
});

function loadDzi(id, callback) {
  if (exiting) {
    log.info('Done!');
    clearInterval(interval);
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

