/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var config = {
  'host'  : '127.0.0.1',
  'port'  : 3306,
  'user'  : 'root',
  'password'  : ''
};
try {
  var _ = require(__dirname + '/config.json');
  for (var i in _) {
    config[i] = _[i];
  }
} catch (e) {
}

exports.config = config;
exports.extend = function (a) {
  var b = {};
  for (var i in config) {
    b[i] = config[i];
  }
  for (var i in a) {
    b[i] =  a[i];
  }
  return b;
};

