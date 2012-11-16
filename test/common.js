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

exports.istravis = process.env.CI ? true : false;
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

var util = require('util');
var Emitter = require('events').EventEmitter;

exports.mockConnection = function () {

  /**
   * @ 请求过的SQL
   */
  var __queries = [];

  /**
   * @ 伪造的数据
   */
  var __Results = [];

  /**
   * @ 连接对象
   */
  var __Objects = [];

  /* {{{ private mocked Connection() */
  var Connection = function () {
    Emitter.call(this);
  };
  util.inherits(Connection, Emitter);

  Connection.prototype.connect = function () {
  };

  Connection.prototype.close = function () {
    this.emit('close');
  };

  Connection.prototype.clone = function () {
    return new Connection();
  };

  Connection.prototype.query = function (sql, tmout, callback) {
    var n = __queries.push(sql);
    var r = [], e = null;
    for (var i = 0; i < __Results.length; i++) {
      var m = (__Results[i])(sql);
      if (m && m.length) {
        r = m[0];
        e = m[1];
        break;
      }
    }
    process.nextTick(function () {
      callback(e, r);
    });
  };
  /* }}} */

  var _me = {};
  _me.create = function () {
    var c = new Connection();
    __Objects.push(c);
    return c;
  };

  _me.makesureCleanAllData = function () {
    __queries = [];
    __Results = [];
  };

  _me.__mockQueryResult = function (p, res, e) {
    __Results.push(function (s) {
      if (s.match(new RegExp(p))) {
        return [res, e];
      }
    });
  };

  _me.__emitEvent = function (i, evt) {
    var c = __Objects[i];
    if (!(c instanceof Connection)) {
      return;
    }

    var a = Array.prototype.slice.call(arguments, 1);
    c.emit.apply(c, a);
  };

  return _me;
};

