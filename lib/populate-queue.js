#!/usr/bin/env node

var async = require('async'),
  cloudApp = require('./common/cloud-app'),
  config = require('../config.json'),
  dziLoader = require('./common/dzi-loader'),
  idLoader = require('./common/id-loader'),
  logging = require('./common/logging'),
  pkgcloud = require('pkgcloud'),
  redis = require('redis');

var interval,
  redisClient,
  storageClient,
  log = logging.getLogger(process.env.ZH_LOG_LEVEL || 'debug'),
  data = {
    skipped: 0,
    blobsToImport: 0,
    dzisToUpload: 0
  },
  exiting = false;

cloudApp.loadProgram(process.argv, function(err, options) {

  if (err) {
    log.error(err);
    process.exit(1);
  }

  storageClient = pkgcloud.providers.rackspace.storage.createClient(options.cloudOptions);

  if (options.redis) {
    redisClient = redis.createClient(options.redis.port, options.redis.host);
    redisClient.auth(process.env.REDIS_PASSWORD, function(err) {
      if (err) {
        process.exit(1);
      }

      initializeQueue(run);
    });
  }
  else {
    redisClient = redis.createClient();
    initializeQueue(run);
  }
});

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

    log.info(data);
    async.forEachLimit(ids, 15, loadDzi, function (err) {
      if (err) {
        log.error(err);
      }

      clearInterval(interval);

      log.info(data);
      doExit();
    });
  });

  interval = setInterval(function () {
    log.info(data);
  }, 30000);
}

function initializeQueue(callback) {
  async.parallel([
    function(next) { redisClient.del([config.blobUrlsKey, config.dzisKey], next); },
    function(next) { redisClient.set(config.totalBlobsKey, 0, next); },
    function(next) { redisClient.set(config.totalBlobsSizeKey, 0, next); },
    function(next) { redisClient.set(config.totalBlobsProcessedKey, 0, next); },
    function(next) { redisClient.set(config.totalDzisKey, 0, next); },
    function(next) { redisClient.set(config.totalDzisSizeKey, 0, next); },
    function(next) { redisClient.set(config.totalDzisProcessedKey, 0, next); }
  ], callback)
}

function loadDzi(id, callback) {
  if (exiting) {
    clearInterval(interval);
    doExit();
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

          data.blobsToImport += result.blobs.length;

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

          data.dzisToUpload++;

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

function doExit(){
  async.parallel([
    function (next) {
      redisClient.set(config.totalBlobsKey, data.blobsToImport, next);
    },
    function (next) {
      redisClient.set(config.totalDzisKey, data.dzisToUpload, next);
    }
  ], function () {
    log.info(data);
    log.info('Done!');
    process.exit(0);
  });
}