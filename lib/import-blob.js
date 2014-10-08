#!/usr/bin/env node

var cloudApp = require('./common/cloud-app'),
  logging = require('./common/logging'),
  pkgcloud = require('pkgcloud'),
  prettyBytes = require('pretty-bytes'),
  redis = require('redis'),
  request = require('request');

var programOpts = cloudApp.loadProgram(process.argv),
  log = logging.getLogger(process.env.IMPORT_LOG_LEVEL || 'debug'),
  config = require('../config.json');

var metrics = {
  blobs: 0, // we always upload a dzi
  size: 0,
  start: Date.now()
};

var LOG_FREQUENCY_SECS = 5,
  exiting = false,
  nextLogTime = Date.now() + 1000 * LOG_FREQUENCY_SECS;

['SIGHUP', 'SIGTERM', 'SIGINT'].forEach(function(signal) {
  process.on(signal, function() {
    log.info('Exiting...');
    exiting = true;
  });
});

var redisClient = redis.createClient(),
  storageClient = pkgcloud.providers.rackspace.storage.createClient(programOpts.cloudOptions);

// make sure we log emitted errors
storageClient.on('log::*', logging.logFunction);

function upload() {
  if (exiting) {
    displayMetrics();
    log.info('Done!');
    process.exit(0);
  }

  redisClient.spop(config.blobUrlsKey, function(err, blob) {
    if (err) {
      restoreBlob(blob);
      return;
    }
    else if (!blob) {
      return;
    }

    log.debug('Fetching blob', config.blobBaseUrl + blob);

    var sourceBlob = request(config.blobBaseUrl + blob);

    var destBlob = storageClient.upload({
      container: 'content',
      remote: sourceBlob.uri.pathname.replace('/content/', '/dzis/')
    });

    sourceBlob.on('response', function(response) {
      metrics.size += parseInt(response.headers['content-length']);
      response.headers = {
        'content-type': response.headers['content-type'],
        'content-length': response.headers['content-length']
      };
    });

    destBlob.on('error', function(err) {
      restoreBlob(blob);
    });

    destBlob.on('success', function() {
      metrics.blobs++;
      log.verbose('Blob Saved', blob);
      upload();
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
  redisClient.sadd(config.blobUrlsKey, blob, function(err) {
    if (err) {
      log.error('couldn\'t requeue blob: ', blob);
      process.exit(1);
    }

    upload();
  });
}

upload();