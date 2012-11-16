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

  this._flag = 0;
  this._conn = null;
  this._name = util.format('%s@%s:%d', options.user, options.host, options.port);

  this.connect(options);
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

Connection.prototype.clone = function () {
  return new Connection(this._conn.config);
};

Connection.prototype.close = function (callback) {

  callback = callback || function () {};

  var _self = this;
  if (_self._flag < 1) {
    return callback();
  }

  _self._flag = -1;
  _self._conn.end(function (error) {
    callback(error);
    _self.emit('close');
  });
};

Connection.prototype.connect = function (options) {

  var _self = this;
  if (_self._flag > 0) {
    return;
  }

  /**
   * @ 连接超时
   */
  var tmout = options.timeout || 100;
  var timer = setTimeout(function () {
    // XXX: this make "Aborted_connects" ++ in mysql server 
    _self._conn._socket.end();
    //
  }, tmout);

  _self._conn = mysql.createConnection(options);
  _self._conn.connect(function (error) {
    clearTimeout(timer);
    timer = null;

    if (error) {
      _self._flag = 0;
      process.nextTick(function () {
        _self.emit('error', _self._error(error));
      });
      return;
    }

    _self._flag = 1;
    _self._conn.on('error', function (error) {
      if (_self._flag < 0 || !error.fatal) {
        return;
      }

      _self._flag = 0;
      _self.emit('close', _self._error(error));
    });
  });

};

Connection.prototype.query = function (sql, timeout, callback) {
  if (!timeout || timeout < 0) {
    this._conn.query(sql, callback);
    return;
  }

  var _self = this;
  var tmout = setTimeout(function () {
    callback(_self._error('QueryTimeout', 'Mysql query timeout after ' + timeout + ' ms'));
    callback = function (e, r) {
      _self.emit('timeout', e, r, sql);
    };
  }, timeout);
  _self._conn.query(sql, function (error, res) {
    clearTimeout(tmout);
    tmout = null;
    callback(error ? _self._error(error) : null, res);
  });
};

exports.create = function (options) {
  return new Connection(options);
};

