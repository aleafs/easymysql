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
  var e = new Error(util.format('%s (%s).', msg || name, this._name));
  e.name = name;
  return e;
};

Connection.prototype.clone = function () {
  return new Connection(this._conn.config);
};

Connection.prototype.close = function (callback) {

  callback = callback || function () {};

  var _self = this;
  if (_self._flag < 1) {
    return callback(_self._error('HaveNotBeenConnected'));
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

  /**
   * @ 重连延迟
   */
  var delay = 10;

  /**
   * onconnect()
   */
  /* {{{ */
  var onconnect = function (e) {

    _self._flag = e ? 0 : 1;
    _self.emit('state', _self._flag);

    if (e) {
      delay = Math.min(10000, 2 * delay);
      setTimeout(_connect, delay);
      _self.emit('error', e);
      return;
    }

    delay = 10;

    _self._conn.on('error', function (error) {
      if (_self._flag < 0 || !error.fatal || 'PROTOCOL_CONNECTION_LOST' !== error.code) {
        return;
      }
      _connect();
    });
  };
  /* }}} */

  /* {{{ */
  var _connect = function () {
    var timer = setTimeout(function () {
      _self._conn.end();
      _self._conn._socket.end();
    }, tmout);

    _self._conn = mysql.createConnection(options);
    _self._conn.connect(function (error) {
      clearTimeout(timer);
      timer = null;
      onconnect(error);
    });
  };
  /* }}} */

  _connect();

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
    callback(error, res);
  });
};

exports.create = function (options) {
  return new Connection(options);
};

