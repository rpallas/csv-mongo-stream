var insert = function (db) {
  return Es.map(
    function (data, callback) {
      if (data.length) {
        var bulk = db.collection('hnet').initializeUnorderedBulkOp();
        data.forEach(function (doc) {
          bulk.insert(doc);
        });
        bulk.execute(callback);
      } else {
        callback();
      }
    }
  );
};

module.exports = {
  insert: insert  
};
