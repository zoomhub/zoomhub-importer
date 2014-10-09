#!/usr/bin/env node

var cloudApp = require('./common/cloud-app'),
  cluster = require('cluster'),
  logging = require('./common/logging'),
  pkgcloud = require('pkgcloud'),
  prettyBytes = require('pretty-bytes'),
  redis = require('redis'),
  request = require('request');

var programOpts = cloudApp.loadProgram(process.argv),
  log = logging.getLogger(process.env.IMPORT_LOG_LEVEL || 'debug'),
  config = require('../config.json');

var LOG_FREQUENCY_SECS = config.logFrequency,
  exiting = false,
  nextLogTime = Date.now() + 1000 * LOG_FREQUENCY_SECS;

var workers = [];

var globalMetrics = {
  blobs: 0, // we always upload a dzi
  size: 0,
  start: Date.now()
};

if (cluster.isMaster) {

  // Listen for dying workers
  cluster.on('exit', function (worker) {
    if (exiting) {
      return;
    }
    // Replace the dead worker,
    // we're not sentimental
    log.warn('Worker ' + worker.id + ' died :(');
    cluster.fork();
  });

  ['SIGHUP', 'SIGTERM', 'SIGINT'].forEach(function (signal) {
    exiting = true;
    process.once(signal, function () {
      log.info('Master Process Received Exit...');
      clearInterval(interval);
    });
  });

  log.info('Spawning ' + config.workerCount + ' workers');

  for (var i = 0; i < config.workerCount; i++) {
    var worker = cluster.fork();
    worker.on('message', function(message) {
      if (message.redisError) {
        process.kill('SIGHUP');
        return;
      }
      globalMetrics.blobs += message.blobs;
      globalMetrics.size += message.size;
    });
  }

  process.on('exit', function() {
    log.info({
      blobs: globalMetrics.blobs,
      runtime: ((Date.now() - globalMetrics.start) / 1000) + 's',
      'blobs/Sec': (globalMetrics.blobs / ((Date.now() - globalMetrics.start) / 1000)).toPrecision(4) * 1,
      size: prettyBytes(globalMetrics.size)
    });
  });

  var interval = setInterval(function() {
    log.info({
      blobs: globalMetrics.blobs,
      runtime: ((Date.now() - globalMetrics.start) / 1000) + 's',
      'blobs/Sec': (globalMetrics.blobs / ((Date.now() - globalMetrics.start) / 1000)).toPrecision(4) * 1,
      size: prettyBytes(globalMetrics.size)
    });
  }, 1000 * LOG_FREQUENCY_SECS)
}
else {
  ['SIGHUP', 'SIGTERM', 'SIGINT'].forEach(function (signal) {
    process.once(signal, function () {
      log.info('Worker Exiting...');
      exiting = true;
    });
  });

  process.on('message', function(message) {
    log.info(message);
    if (message.shutdown) {
      log.info('Worker Graceful Shutdown...');
      exiting = true;
    }
  });

  var metrics = {
    blobs: 0, // we always upload a dzi
    size: 0,
    start: Date.now()
  };

  var redisClient = redis.createClient(),
    storageClient = pkgcloud.providers.rackspace.storage.createClient(programOpts.cloudOptions);

  // make sure we log emitted errors
  storageClient.on('log::*', logging.logFunction);

  redisClient.on('error', function(err) {
    log.error(JSON.stringify(err));
    process.send({ redisError: true });
  });

  redisClient.on('ready', function() {
    upload();
  });

  function upload() {
    if (exiting) {
      log.info('Worker Done!');
      displayMetrics();
      process.exit(0);
    }

    redisClient.spop(config.blobUrlsKey, function (err, blob) {
      if (err) {
        log.warn('Error connecting to redis', err.toString());
        setTimeout(function() {
          log.info('ping');
          upload();
        }, 5000);
        return;
      }
      else if (!blob) {
        log.warn('No Blob, exiting...');
        process.exit(0);
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
        response.headers = {
          'content-type': response.headers['content-type'],
          'content-length': response.headers['content-length']
        };
      });

      destBlob.on('error', function (err) {
        restoreBlob(blob);
      });

      destBlob.on('success', function () {
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
    process.send(metrics);
    metrics.blobs = 0;
    metrics.size = 0
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
}
