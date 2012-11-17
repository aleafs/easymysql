/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var mysql = require('mysql');
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
  this._conn = mysql.createConnection(options);
  this._name = util.format('%s@%s:%d', options.user, options.host, options.port);

  this.connect((options && options.timeout) ? options.timeout : 100);
};
util.inherits(Connection, EventEmitter);

Connection.prototype.connect = function (timeout) {

  if (this._flag > 0) {
    return true;
  }

  var _self = this;
  _self._flag = 1;

  /**
   * @ 连接超时
   */
  var tmout = (~~timeout) || 100;
  var timer = setTimeout(function () {
    // XXX: this make "Aborted_connects" ++ in mysql server 
    _self._conn._socket.end();
    _self.emit('error', _self._error('ConnectTimeout', 
        'Connect to mysql server timeout after ' + tmout + ' ms'));
  }, tmout);

  _self._conn.removeAllListeners();
  ['error', 'close', 'end'].forEach(function (i) {
    _self._conn.on(i, function (e) {
      if (_self._flag < 1) {
        return;
      }

      e && _self.emit('error', _self._error(e));
      if ((e && e.fatal) || 'error' !== i) {
        _self._flag = 0;
        _self._conn.end();
        _self.emit('close');
      }
    });
  });

  _self._conn.connect(function (e) {
    clearTimeout(timer);
    timer = null;
    if (!e) {
      _self._flag = 2;
    } else {
      _self._flag = 0;
      process.nextTick(function () {
        _self.emit('error', _self._error(e));
      });
    }
  });
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

Connection.prototype.close = function (callback) {

  callback = callback || function () {};
  if (this._flag < 1) {
    return callback();
  }
  var _self = this;

  _self._flag = -1;
  _self._conn.end(function (error) {
    _self.emit('close');
    callback(error);
  });
};

Connection.prototype.query = function (sql, timeout, callback) {

  if (!timeout || timeout < 1) {
    return this._conn.query(sql, callback);
  }

  var _self = this;
  var timer = setTimeout(function () {
    callback(_self._error('QueryTimeout', 'Mysql query timeout after ' + timeout + ' ms'));
    callback = function (e, r) {
      _self.emit('late', e, r, sql);
    };
  }, timeout);
  _self._conn.query(sql, function (error, res) {
    clearTimeout(timer);
    timer = null;
    callback(error ? _self._error(error) : null, res);
  });
};

exports.create = function (options) {
  return new Connection(options);
};

