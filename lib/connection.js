/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var mysql = require('mysql-robin');
var EventEmitter = require('events').EventEmitter;

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
  this._name = util.format('%s@%s:%d', options.user, options.host, options.port);
  this._conn = mysql.createConnection(options);

  var _self = this;
  this._conn.on('error', function (e) {
    if (e && e.fatal && _self._flag > -1) {
      _self.close();
    }
    _self.emit('error', _self._error(e));
  });
};
util.inherits(Connection, EventEmitter);

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
  this._conn.end();
};

Connection.prototype.query = function (sql, timeout, callback) {

  var _self = this;
  if (!timeout || timeout < 1) {
    return this._conn.query(sql, function (e, r) {
      callback(e ? _self._error(e) : null, r);
    });
  }

  var timer = setTimeout(function () {
    callback(_self._error('QueryTimeout', 'Mysql query timeout after ' + timeout + ' ms'));
    callback = function (e, r) {
      _self.emit('late', e, r, sql);
    };
  }, timeout);
  _self._conn.query(sql, function (e, r) {
    clearTimeout(timer);
    timer = null;
    callback(e ? _self._error(e) : null, r);
  });
};

exports.create = function (options) {
  return new Connection(options);
};

