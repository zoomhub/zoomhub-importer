var Etcd = require('node-etcd'),
  program = require('commander');

exports.loadProgram = function(args, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = [];
  }

  program
    .version('0.0.1')
    .option('-c, --command [populate|run]', 'Command')
    .option('-u, --username [username]', 'Username')
    .option('-p, --password [password]', 'Password')
    .option('-r, --region [region]', 'Region')
    .option('--useInternal', 'Use Local Service Interface')
    .option('--useEtcd', 'Use Etcd for Service Discovery');

  options.forEach(function(option) {
    program.option(option);
  });

  program.parse(args);

  if (!program.username) {
    program.username = process.env.RAX_USERNAME;
  }

  if (!program.password) {
    program.password = process.env.RAX_PASSWORD;
  }

  if (!program.region) {
    program.region = process.env.RAX_REGION;
  }

  if (!program.username && !program.password && !program.region) {
    program.help();
  }

  var programOptions = {
    program: program,
    cloudOptions: {
      username: program.username,
      password: program.password,
      useInternal: program.useInternal,
      region: program.region
    }
  };

  if (!program.useEtcd) {
    callback(null, programOptions);
    return;
  }

  var e = new Etcd(process.env.ETCD_PORT_4001_TCP_ADDR || '172.17.42.1',
      process.env.ETCD_PORT_4001_TCP_PORT || 4001);

  e.get('/services/app-redis', function(err, data) {
    if (err) {
      callback(null, programOptions);
      return;
    }

    programOptions.redis = {
      host: data.node.value.split(':')[0],
      port: data.node.value.split(':')[1]
    };

    callback(null, programOptions);
  });
};
