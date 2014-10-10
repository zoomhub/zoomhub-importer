#!/usr/bin/env node

var async = require('async'),
  cloudApp = require('./common/cloud-app'),
  config = require('../config.json'),
  logging = require('./common/logging'),
  pkgcloud = require('pkgcloud'),
  prettyBytes = require('pretty-bytes'),
  redis = require('redis'),
  request = require('request');

var interval,
  metrics,
  redisClient,
  storageClient,
  LOG_FREQUENCY_SECS = config.logFrequency,
  log = logging.getLogger(process.env.ZH_LOG_LEVEL || 'debug'),
  exiting = false,
  nextLogTime = Date.now() + 1000 * LOG_FREQUENCY_SECS;

metrics = {
  dzis: 0,
  size: 0,
  start: Date.now()
};

config.sleepBackoff = process.env.ZH_SLEEP_BACKOFF || 30000;

['SIGHUP', 'SIGTERM', 'SIGINT'].forEach(function (signal) {
  process.once(signal, function () {
    log.info('Exiting...');
    exiting = true;
  });
});

// First load our environment and prepare the clients.
cloudApp.loadProgram(process.argv, function (err, options) {

  if (err) {
    log.error('Unable to get config for program', err.toString());
    process.exit(1);
  }

  storageClient = pkgcloud.providers.rackspace.storage.createClient(options.cloudOptions);

  if (options.redis) {
    redisClient = redis.createClient(options.redis.port, options.redis.host);
    redisClient.auth(process.env.ZH_REDIS_PASSWORD, function (err) {
      if (err) {
        log.error('Unable to auth to redis', err.toString());
        process.exit(1);
      }

      run();
    });
  }
  else {
    redisClient = redis.createClient();
    run();
  }
});

function run() {
  // make sure we log emitted errors
  storageClient.on('log::*', logging.logFunction);

  redisClient.on('error', function (err) {
    log.error('Redis Error', err.toString());
    process.exit(1);
  });

  redisClient.on('ready', function () {
    upload();
  });
}

function upload() {
  if (exiting) {
    log.info('Done!');
    displayMetrics();
    process.exit(0);
  }

  redisClient.spop(config.dzisKey, function (err, dzi) {
    if (err) {
      log.error('Error popping id from redis', err.toString());
      process.exit(1);
      return;
    }
    else if (!dzi) {
      log.warn('No DZI, Sleeping...');
      setTimeout(function () {
        upload();
      }, config.sleepBackoff);
      return;
    }

    dzi = JSON.parse(dzi);

    var destBlob = storageClient.upload({
      container: 'content',
      remote: '/dzis/' + dzi.id + '.dzi',
      contentType: 'application/xml',
      size: dzi.xml.length
    });

    destBlob.on('error', function (err) {
      restoreDzi(dzi);
    });

    destBlob.on('success', function (file) {
      metrics.dzis++;
      metrics.size+= dzi.xml.length;
      async.parallel([
        function(next) { redisClient.incr(config.totalDzisProcessedKey, next); },
        function(next) { redisClient.incrby(config.totalDzisSizeKey, dzi.xml.length, next); }
      ], function(err) {
        if (err) {
          log.error('Unable to increment total dzis count', err.toString());
          process.exit(1);
          return;
        }

        log.debug('Dzi Saved', file.toJSON());
        upload();
      });
    });

    destBlob.end(dzi.xml);
  });

  if (Date.now() >= nextLogTime) {
    displayMetrics();
    nextLogTime = Date.now() + 1000 * LOG_FREQUENCY_SECS;
  }
}

function displayMetrics() {
  log.info({
    dzis: metrics.dzis,
    runtime: ((Date.now() - metrics.start) / 1000) + 's',
    'dzis/Sec': (metrics.dzis / ((Date.now() - metrics.start) / 1000)).toPrecision(4) * 1,
    size: prettyBytes(metrics.size)
  });
}

function restoreDzi(dzi) {
  log.warn('Restoring dzi into queue', dzi);
  redisClient.sadd(config.dzisKey, JSON.stringify(dzi), function (err) {
    if (err) {
      log.error('couldn\'t requeue dzi: ', dzi);
      process.exit(1);
      return;
    }

    upload();
  });
}

