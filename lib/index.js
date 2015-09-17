var Async = require('async');
var Fs = require('fs');
var MongoClient = require('mongodb').MongoClient;

var Loader = require('./loader');

exports = module.exports = {
  load: function (sourcePath, options, callback) {
    Async.auto({
      db: function (callback) {
        console.log('opening db connection');
        MongoClient.connect(options.mongoURI, callback);
      },
      subDirectory: function (callback) {
        Fs.readdir(sourcePath, callback);
      },
      loadData: ['db', 'subDirectory', function (callback, results) {
        Async.each(results.subDirectory, load(results.db), callback);
      }],
      cleanUp: ['db', 'loadData', function (callback, results) {
        console.log('closing db connection');
        results.db.close(callback);
      }]
    }, callback); 
  }
}
