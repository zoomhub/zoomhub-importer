#!/usr/bin/env node

var cloudApp = require('./common/cloud-app'),
  config = require('../config.json'),
  logging = require('./common/logging'),
  pkgcloud = require('pkgcloud'),
  prettyBytes = require('pretty-bytes'),
  redis = require('redis'),
  request = require('request');

var metrics,
  redisClient,
  storageClient,
  LOG_FREQUENCY_SECS = config.logFrequency,
  log = logging.getLogger(process.env.ZH_LOG_LEVEL || 'debug'),
  exiting = false,
  nextLogTime = Date.now() + 1000 * LOG_FREQUENCY_SECS;

metrics = {
  blobs: 0,
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

  redisClient.spop(config.blobUrlsKey, function (err, blob) {
    if (err) {
      log.error('Error popping id from redis', err.toString());
      process.exit(1);
      return;
    }
    else if (!blob) {
      log.warn('No Blob, Sleeping...');
      setTimeout(function() {
        upload();
      }, config.sleepBackoff);
      return;
    }

    log.debug('Fetching blob', config.blobBaseUrl + blob);

    var sourceBlob = request(config.blobBaseUrl + blob);

    var destBlob = storageClient.upload({
      container: 'content',
      remote: sourceBlob.uri.pathname.replace('/content/', '/dzis/')
    });

    sourceBlob.on('response', function (response) {
      metrics.size += parseInt(response.headers['content-length']);
      redisClient.incrby(config.totalBlobsSizeKey, parseInt(response.headers['content-length']));
      response.headers = {
        'content-type': response.headers['content-type'],
        'content-length': response.headers['content-length']
      };
    });

    destBlob.on('error', function (err) {
      restoreBlob(blob);
    });

    destBlob.on('success', function (file) {
      metrics.blobs++;
      redisClient.incr(config.totalBlobsProcessedKey, function (err) {
        if (err) {
          log.error('Unable to increment total blobs count', err.toString());
          process.exit(1);
          return;
        }

        log.debug('Blob Saved', file.toJSON());
        upload();
      });
    });

    sourceBlob.pipe(destBlob);
  });

  if (Date.now() >= nextLogTime) {
    displayMetrics();
    nextLogTime = Date.now() + 1000 * LOG_FREQUENCY_SECS;
  }
}

function displayMetrics() {
  log.info({
    blobs: metrics.blobs,
    runtime: ((Date.now() - metrics.start) / 1000) + 's',
    'blobs/Sec': (metrics.blobs / ((Date.now() - metrics.start) / 1000)).toPrecision(4) * 1,
    size: prettyBytes(metrics.size)
  });
}

function restoreBlob(blob) {
  log.warn('Restoring blob into queue', blob);
  redisClient.sadd(config.blobUrlsKey, blob, function (err) {
    if (err) {
      log.error('couldn\'t requeue blob: ', blob);
      process.exit(1);
    }

    upload();
  });
}

