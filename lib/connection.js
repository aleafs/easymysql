/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var mysql = require('mysql-robin');
var EventEmitter = require('events').EventEmitter;
var sqlString = require('mysql-robin/lib/protocol/SqlString');

var noop = function () {};

/**
 * @ Connection
 */
var Connection = function (options) {

  EventEmitter.call(this);

  /**
   * 1 : 正常使用
   * -1: 准备断开
   */
  options.port = options.port || 3306;
  this._name = util.format('%s@%s:%d', options.user, options.host, options.port);
  this._conn = mysql.createConnection(options);
  this._flag = 1;

  this._fatalError = null;

  this._socketTimeout = isNaN(+options.sockettimeout) ? 60000 /* 1 min */ : options.sockettimeout;

  var _self = this;

  this._conn.on('error', function (e) {
    e = _self._error(e);
    if (e.fatal) {
      _self._fatalError = e;
    }
    _self.emit('error', e);
  });
};
util.inherits(Connection, EventEmitter);

Connection.prototype.connected = function () {
  return this._flag > 0;
};

Connection.prototype._error = function (name, msg) {
  var e;
  if (name instanceof Error) {
    e = name;
    e.name = (e.name && 'Error' !== e.name) ? e.name : 'MysqlError';
  } else {
    e = new Error(msg || name);
    e.name = name;
  }
  e.message = util.format('%s (%s)', e.message, this._name);
  return e;
};

Connection.prototype.close = function (cb) {
  if (this._flag < 0) {
    return;
  }
  this._flag = -1;

  var _self = this;
  this._conn.end(function () {
    cb && cb();
  });
};

Connection.prototype.query = function (sql, timeout, callback) {
  var _self = this;

  if ((typeof sql) === 'object' && sql.params) {
    sql = _self.format(sql.sql, sql.params);
  }

  if (_self._fatalError) {
    return process.nextTick(function () {
      callback(_self._fatalError);
    });
  }

  if (_self._flag > 0) {
    _self._conn._implyConnect();
    _self._conn._socket.setTimeout(_self._socketTimeout);
    _self._conn._socket.removeAllListeners('timeout');

    _self._conn._socket.once('timeout', function () {
      var e = _self._error('SocketTimeout', 'Mysql socket timeout after ' + _self._socketTimeout + ' ms.');
      e.fatal = true;
      callback(e);
      callback = noop;
    });
  }

  if (!timeout || isNaN(+timeout) || timeout < 1) {
    return _self._conn.query(sql, function (e, r) {
      callback(e ? _self._error(e) : null, r);
      callback = noop;
    });
  }

  var timer = setTimeout(function () {
    var e = _self._error('QueryTimeout', 'Mysql query timeout after ' + timeout + ' ms');
    e.fatal = true;
    callback(e);
    callback = noop;
  }, timeout);

  _self._conn.query(sql, function (e, r) {
    clearTimeout(timer);
    timer = null;
    callback(e ? _self._error(e) : null, r);
    callback = noop;
  });
};

Connection.prototype.format = function (sql, params) {
  return sql.replace(/:(\w+)/g, function (w, i) {
    return sqlString.escape(params[i]);
  });
};

exports.create = function (options) {
  return new Connection(options);
};

