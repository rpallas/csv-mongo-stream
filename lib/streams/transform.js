var transform = function (directory) {
  return Es.map(function (data, callback) {
    data.siteRef = Mapping[directory];
    data.epoch = parseInt((data.TheTime - 25569) * 86400) + 6 * 3600;
    callback(null, data);
  });
};

module.exports = {
  transform: transform
};
