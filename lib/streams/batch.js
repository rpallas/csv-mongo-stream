var batch = function (batchSize) {
  batchSize = batchSize || 1000;
  var batch = [];

  return Es.through(
    function write (data) {
      batch.push(data);
      if (batch.length === batchSize) {
        this.emit('data', batch);
        batch = [];
      }
    },
    function end () {
      if (batch.length) {
        this.emit('data', batch);
        batch = [];
      }
      this.emit('end');
    }
  );
};

module.exports = {
  batch: batch
};
