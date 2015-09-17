var Csv = require('csv-streamify');
var Es = require('event-stream');
var Fs = require('fs');

var Streams = require('./streams');

var load = function (db, options) {
  return function (directory, callback) {
    var basePath = options.sourcePath + '/' + directory;
    Async.waterfall([
      function (callback) {
        Fs.readdir(basePath, callback);
      },
      function (files, callback) {
        console.log('loading ' + files.length + ' files from ' + directory);
        Async.each(files, function (file, callback) {
          Fs.createReadStream(basePath + '/' + file)
            .pipe(Csv({objectMode: true, columns: true}))
            .pipe(Streams.transform(directory))
            .pipe(Streams.batch(200))
            .pipe(Streams.insert(db).on('end', callback));
        }, callback);
      }
    ], callback);
  };
};

module.exports = {
  load: load  
};

