/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var events = require('events');
var mysql = require('mysql');

var Connection = function (options) {
  events.EventEmitter.call(this);

  this._flag = 0;
  this._conn = mysql.createConnection(options);
  this.connect();
};
util.inherits(Connection, events.EventEmitter);

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
  });
};

Connection.prototype.query = function (sql, options, callback) {
  this._conn.query(sql, options, callback);
};

Connection.prototype.clone = function () {
  return new Connection(this._conn.config);
};

Connection.prototype.close = function (callback) {

  callback = callback || function () {};

  var _self = this;
  if (_self._flag < 1) {
    return callback(new Error('NotConnected'));
  }

  _self._flag = -1;
  _self._conn.end(function (error) {
    callback(error);
    _self.emit('close');
  });
};

exports.create = function (config, options) {

  var _options = {
    'maxconnection' : 4,      /**<  最大连接数  */
    'max_idletime'  : 30000,
  };
  for (var i in options) {
    _options[i] = options[i];
  }

  /**
   * @ 心跳连接
   */
  var c_heart = new Connection(config);
  c_heart.on('close', function () {
    c_heart.connect();
  });
  (function heartbeat () {
    c_heart.query('', function (error, res) {
    });
  })();

  /**
   * @ 工作连接
   */
  var c_query = {};

  /**
   * @ 连接计数器，只增不减
   */
  var _conidx = 0;

  /**
   * @ 当前连接数，c_query中的元素个数
   */
  var _connum = 0;

  /**
   * @ 请求队列
   */
  var w_queue = [];

  /**
   * @ 空闲连接
   */
  var w_stack = [];

  var _next = function (o, m, i) {
    if (w_queue.length < 1) {
      w_stack.push(i);
      o.emit('free', w_stack.length);
      return;
    }
    m.query.call(w_queue.shift());
  };

  var Agent = function () {
    events.EventEmitter.call(this);
  };
  util.inherits(Agent, events.EventEmitter);

  Agent.prototype.isfree = function () {
    return w_stack.length + _options.maxconnection - _connum;
  };

  /* {{{ public prototype query() */

  Agent.prototype.query = function (sql, options, callback) {
    var i, m;
    while (!(i && m)) {
      i = w_stack.pop();
      if (i) {
        m = c_query[i];
      }
    }

    if (!m && _connum < _options.maxconnection) {
      m = c_heart.clone();
      i = _conidx;

      c_query[i] = m;
      _connum++;
      _conidx++;

      m.on('close', function () {
        delete c_query[i];
        _connum = Object.keys(c_query).length;
      });
    }

    if (m) {
      var _self = this;
      m.query(sql, options, function (error, res) {
        callback(error, res);
        _next(_self, m, i);
      });
      return;
    }

    w_queue.push(arguments);
  };
  /* }}} */

  return new Agent();
};

