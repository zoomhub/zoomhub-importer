var DeepZoomImage = require('deepzoomtools'),
    request = require('request'),
    url = require('url'),
    xml2js = require('xml2js');

var num = function (x) {
  return parseInt(x, 10);
};

var parser = new xml2js.Parser({
  strict: false
});

exports.loadDzi = function (id, callback) {

  var source = 'http://cache.zoom.it/content/' + id + '.dzi';

  request(source, function (err, response, body) {
    if (err) {
      return callback(err);
    }
    else if (response.statusCode === 404) {
      return callback({ id: id, noDziFound: true });
    }
    else if (response.statusCode !== 200) {
      return callback({ unexpectedStatusCode: true, statusCode: response.statusCode });
    }

    var response = {
      id: id,
      dzi: body,
      blobs: [],
      dziPath: url.parse(source).pathname
    };

    parser.parseString(body, function (err, result) {
      if (err || result.ERROR) {
        return callback(err ? err : result.ERROR);
      }

      var data = {
        tileSize: num(result['IMAGE']['$']['TILESIZE']),
        overlap: num(result['IMAGE']['$']['OVERLAP']),
        format: result['IMAGE']['$']['FORMAT'],
        width: num(result['IMAGE']['SIZE'][0]['$']['WIDTH']),
        height: num(result['IMAGE']['SIZE'][0]['$']['HEIGHT']),
        id: id
      };

      var descriptor = new DeepZoomImage(source, data.width, data.height, data.tileSize, data.overlap, data.format);

      descriptor.levels.forEach(function (level) {
        for (var column = 0; column < level.numColumns; column++) {
          for (var row = 0; row < level.numRows; row++) {
            var tile = descriptor.getTile(level.index, column, row);
            response.blobs.push(tile.url.substring(20));
          }
        }
      });

      callback(null, response);

    });
  });

};