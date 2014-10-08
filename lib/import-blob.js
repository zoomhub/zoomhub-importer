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

var uptime = {
  blobs: 0, // we always upload a dzi
  size: 0,
  start: new Date().getTime()
};

var redisClient = redis.createClient(),
  storageClient = pkgcloud.providers.rackspace.storage.createClient(programOpts.cloudOptions);

// make sure we log emitted errors
storageClient.on('log::*', logging.logFunction);

function upload() {

  redisClient.spop(config.blobUrlsKey, function(err, blob) {
    if (err) {
      restoreBlob(blob);
      return;
    }
    else if (!blob) {
      return;
    }

    var sourceBlob = request(config.blobBaseUrl + blob);

    var destBlob = storageClient.upload({
      container: 'content',
      remote: sourceBlob.uri.pathname.replace('/content/', '/dzis/')
    });

    sourceBlob.on('response', function(response) {
      response.headers = {
        'content-type': response.headers['content-type'],
        'content-length': response.headers['content-length']
      };
    });

    destBlob.on('error', function(err) {
      restoreBlob(blob);
    });

    destBlob.on('success', function() {
      log.verbose('Blob Saved', blob);
      upload();
    });

    sourceBlob.pipe(destBlob);
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