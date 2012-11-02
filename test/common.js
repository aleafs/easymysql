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

exports.istravis = process.env.CI ? true : false;

var Server = require(__dirname + '/../node_modules/mysql/test/FakeServer.js');
var Packet = require(__dirname + '/../node_modules/mysql/lib/protocol/packets');
console.log(Packet);
exports.liteServer = function (port, cb) {

  var _me = new Server();
  _me.listen(port, function (e) {
    if (e) {
      throw e;
    }
  });

  _me.on('connection', function (con) {
    con.handshake();
    con.on('query', function (packet) {
      console.log(packet);
      // XXX: 回写
      con._sendPacket(new Packet.ResultSetHeaderPacket({
        'fieldCount' : 1,
      }));
      con._sendPacket(new Packet.EmptyPacke());
    });
  });

  return {
    'close' : function () {
      _me.destroy();
    },
  };
};

