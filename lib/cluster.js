/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var events = require('events');
var mysql = require('mysql');

var READONLY  = 1;
var WRITABLE  = 2;

/**
 * @ Connection
 */
var Connection = function (options) {

  events.EventEmitter.call(this);

  this._flag = 0;
  this._conn = mysql.createConnection(options);
  this.connect();
};
util.inherits(Connection, events.EventEmitter);

/* {{{ Connection prototype clone() */

Connection.prototype.clone = function () {
  return new Connection(this._conn.config);
};
/* }}} */

/* {{{ Connection prototype close() */

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
/* }}} */

/* {{{ Connection prototype connect() */

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
/* }}} */

/* {{{ Connection prototype query() */

Connection.prototype.query = function (sql, timeout, callback) {
  if (!timeout || timeout < 0) {
    this._conn.query(sql, callback);
    return;
  }

  var _self = this;
  var tmout = setTimeout(function () {
    var e = new Error('Mysql query timeout after ' + timeout + ' ms.');
    e.name = 'QueryTimeout';
    callback(e);
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
/* }}} */

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

  /**
   * @ 数据库状态
   */
  var _status = 0;

  (function heartbeat () {
    c_heart.query('SHOW VARIABLES LIKE "read_only"', 100, function (error, res) {
      if (error) {
        return;
      }

      _status = READONLY;
      if (((res.shift() || {}).Value + '').match(/^(off)$/i)) {
        _status |= WRITABLE;
      }

      setTimeout(heartbeat, 1000);
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
      o.emit('free', _free(), _status);
      return;
    }

    var q = w_queue.shift();
    m.query(q[0], q[1], function (error, res) {
      (q[2])(error, res);
      _next(o, m, i);
    });
  };

  var Agent = function () {
    events.EventEmitter.call(this);
  };
  util.inherits(Agent, events.EventEmitter);

  Agent.prototype._status = function () {
    return [_free(), _status];
  };

  Agent.prototype.query = function (sql, timeout, callback) {
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

    if ('function' === (typeof timeout)) {
      callback = timeout;
      timeout = null;
    }

    if (!m) {
      w_queue.push([sql, timeout, callback]);
    } else {
      var _self = this;
      m.query(sql, timeout, function (error, res) {
        callback(error, res);
        _next(_self, m, i);
      });
    }
  };

  return new Agent();
};

exports.createPool = function (options) {

  var MysqlPool = function (options) {
    events.EventEmitter.call(this);
  };
  util.inherits(MysqlPool, events.EventEmitter);

  MysqlPool.prototype.addserver = function (config) {
  };

  MysqlPool.prototype.query = function (sql, timeout, callback) {
  };

  return new MysqlPool();
};

