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

Connection.prototype.query = function (sql, callback) {
  this._conn.query(sql, callback);
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
    c_heart.query('SHOW DATABASES', function (error, res) {
    });
  })();

  /**
   * @ 工作连接
   */
  var c_query = [];

  /**
   * @ 请求队列
   */
  var w_queue = [];

  /**
   * @ 空闲连接
   */
  var w_stack = [];

  var _free = function () {
    return w_stack.length + _options.maxconnection - c_query.length;
  };

  var _next = function (o, m, i) {
    if (w_queue.length < 1) {
      w_stack.push(i);
      o.emit('free', _free());
      return;
    }

    var q = w_queue.shift();
    m.query(q[0], function (error, res) {
      (q[1])(error, res);
      _next(o, m, i);
    });
  };

  var Agent = function () {
    events.EventEmitter.call(this);
  };
  util.inherits(Agent, events.EventEmitter);

  /* {{{ public prototype query() */

  Agent.prototype.query = function (sql, callback) {
    var i, m;
    do {
      i = w_stack.pop();
      if (i) {
        m = c_query[i];
      }
    } while (i && !m);

    if (!m && c_query.length < _options.maxconnection) {
      m = c_heart.clone();
      i = c_query.push(m) - 1;
      m.on('close', function () {
        var a = [];
        c_query.forEach(function (o) {
          if (o._flag >= 0) {
            a.push(o);
          }
        });
        c_query = a;
      });
    }

    if (!m) {
      w_queue.push([sql, callback]);
    } else {
      var _self = this;
      m.query(sql, function (error, res) {
        callback(error, res);
        _next(_self, m, i);
      });
    }
  };
  /* }}} */

  return new Agent();
};

