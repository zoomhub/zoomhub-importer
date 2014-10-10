#!/usr/bin/env node

var async = require('async'),
  cloudApp = require('./common/cloud-app'),
  config = require('../config.json'),
  logging = require('./common/logging'),
  redis = require('redis'),
  Stats = require('./common/stats').Stats;

var interval,
  redisClient,
  LOG_FREQUENCY_SECS = config.logFrequency * 1000,
  log = logging.getLogger(process.env.ZH_LOG_LEVEL || 'debug'),
  stats;

cloudApp.loadProgram(process.argv, function (err, options) {

  if (err) {
    log.error(err);
    process.exit(1);
  }

  if (options.redis) {
    redisClient = redis.createClient(options.redis.port, options.redis.host);
    redisClient.auth(process.env.ZH_REDIS_PASSWORD, function (err) {
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
});

function run() {
  ['SIGHUP', 'SIGTERM', 'SIGINT'].forEach(function (signal) {
    process.on(signal, function () {
      process.exit(0);
    });
  });

  stats = {
    blobs: new Stats(redisClient, 'Blobs'),
    dzis: new Stats(redisClient, 'Dzis')
  };

  async.parallel([
    function(next) { stats.blobs.init(next); },
    function(next) { stats.dzis.init(next); },
  ], function (err) {
    if (err) {
      log.error('Unable to load count from redis', err.toString());
    }

    //log.info(stats);

    interval = setInterval(function () {
      async.parallel([
        function(next) { stats.blobs.load(next); },
        function(next) { stats.dzis.load(next); },
      ], function (err) {
        if (err) {
          log.error('Unable to load count from redis', err.toString());
          clearInterval(interval);
        }

        var dziStats = stats.dzis.getStats();
        var blobStats = stats.blobs.getStats();

        process.stdout.write('Blobs (' + dziStats.perSecondAverage + '/s)\t' + blobStats.percentComplete + '% ' + blobStats.totalProcessed + '/' + blobStats.total);
        process.stdout.write('\t\tDZIs (' + dziStats.perSecondAverage + '/s)\t' + dziStats.percentComplete + '% ' + dziStats.totalProcessed + '/' + dziStats.total + '\n');

      });
    }, LOG_FREQUENCY_SECS)

  });
}
