/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var mysql = require('mysql-robin');
var EventEmitter = require('events').EventEmitter;
var sqlString = require('mysql-robin/lib/protocol/SqlString');

var SOCKET_TIMEOUT = 30000;

var noop = function () {};

/**
 * @ Connection
 */
var Connection = function (options) {

  EventEmitter.call(this);

  /**
   * 0 : 未连接
   * 1 : 正在连接
   * 2 : 连接成功
   * -1: 准备断开
   */
  this._flag = 0;
  options.port = options.port || 3306;
  this._name = util.format('%s@%s:%d', options.user, options.host, options.port);
  this._conn = mysql.createConnection(options);

  this._fatalError = null;

  this._flag = 1;
  var _self = this;

  this._conn.connect(function (e) {
    if (!e) {
      _self._conn._socket.setTimeout(SOCKET_TIMEOUT);
      _self._flag = 2;
    } else {
      e = _self._error(e);
      e.fatal = true;
      _self._fatalError = e;
    }
    _self.emit('connect', e);
  });

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
  return 2 === this._flag;
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

Connection.prototype.close = function () {

  if (this._flag < 0) {
    return;
  }

  this._flag = -1;

  var _self = this;
  var timer = setTimeout(function () {
    _self._conn.destroy();
  }, 10);

  this._conn.end(function () {
    clearTimeout(timer);
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

  if (!timeout || isNaN(+timeout) || timeout < 1) {
    _self._conn._socket.removeAllListeners('timeout');
    _self._conn._socket.once('timeout', function () {
      var e = _self._error('SocketTimeout', 'Mysql query timeout after ' + SOCKET_TIMEOUT + ' ms');
      e.fatal = true;
      callback(e);
      callback = noop;
    });
    return _self._conn.query(sql, function (e, r) {
      callback(e ? _self._error(e) : null, r);
      callback = noop;
    });
  }

  var timer = setTimeout(function () {
    callback(_self._error('QueryTimeout', 'Mysql query timeout after ' + timeout + ' ms'));
    _self.emit('timeout', sql);
    callback = noop;
  }, timeout);

  _self._conn.query(sql, function (e, r) {
    clearTimeout(timer);
    timer = null;
    callback(e ? _self._error(e) : null, r);
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

