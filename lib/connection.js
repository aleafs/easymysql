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
  this._conn = mysql.createConnection(options);
  this._name = util.format('%s@%s:%d', this._conn.config.user,
      this._conn.config.host, this._conn.config.port);

  this.connect();
};
util.inherits(Connection, EventEmitter);

Connection.prototype._error = function (name, msg) {
  var e = new Error(util.format('%s/%s', this._name, msg || name));
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

Connection.prototype.connect = function () {

  var _self = this;
  if (_self._flag > 0) {
    return;
  }

  (function errorHandle(o) {
    o._conn.on('error', function (error) {
      if (o._flag < 0) {
        return;
      }

      o.emit('error', error);
      if (!error.fatal || 'PROTOCOL_CONNECTION_LOST' !== error.code) {
        return;
      }

      o._conn = mysql.createConnection(o._conn.config);
      errorHandle(o);
    });
  })(_self);

  _self._conn.connect(function (error) {
    _self._flag = error ? 0 : 1;
    _self.emit('connect', error);
  });
};

Connection.prototype.query = function (sql, timeout, callback) {
  if (!timeout || timeout < 0) {
    this._conn.query(sql, callback);
    return;
  }

  var _self = this;
  var tmout = setTimeout(function () {
    callback(_self._error('QueryTimeout', 'Mysql query timeout after ' + timeout + ' ms.'));
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

