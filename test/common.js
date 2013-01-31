/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var Clone = require('clone');

var config = {
  'host'  : '127.0.0.1',
  'port'  : 3306,
  'user'  : 'root',
  'password'  : '',
  'timeout' : 1000,       /**<  connect timeout in ms */
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
    this._name  = 'test';
    this._flag = 1;
    var _self = this;
  };
  util.inherits(Connection, Emitter);

  Connection.prototype.connected = function () {
    return this._flag > 0;
  };

  Connection.prototype.close = function () {
    this.emit('close');
  };

  Connection.prototype._setFlag = function (i) {
    this._flag = i;
  }

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
    if (tmout < 10) {
      process.nextTick(function () {
        callback(e, r);
      });
    } else {
      setTimeout(function () {
        callback(e, r);
      }, 3 + tmout);
    }
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
    __Objects = [];
  };

  _me.__mockQueryResult = function (p, res, e) {
    __Results.push(function (s) {
      if (s.match(new RegExp(p))) {
        return [Clone(res), Clone(e)];
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

  _me.__setFlag = function (i, flag) {
    var c = __Objects[i];
    if (!(c instanceof Connection)) {
      return;
    }
    c._setFlag(flag);
  };

  _me.__connectionNum = function () {
    return __Objects.length;
  };

  return _me;
};

