/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var events = require('events');
var mysql = require('mysql');

var READONLY  = 1;
var WRITABLE  = 2;

var QueryError = function (name, msg) {
  var e = new Error(msg || name);
  e.name = name;
  return e;
};

/**
 * @ Connection
 */
var Connection = function (options) {

  events.EventEmitter.call(this);

  this._flag = 0;
  this._conn = mysql.createConnection(options);
  this._name = util.format('%s@%s:%d', this._conn.config.user,
      this._conn.config.host, this._conn.config.port);

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
    return callback(QueryError('HaveNotBeenConnected'));
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
    callback(QueryError('QueryTimeout', 'Mysql query timeout after ' + timeout + ' ms.'));
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

var createAgent = exports.create = function (config, options) {

  var _options = {
    'maxconnection' : 4,
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
    m.query(q[0], null, function (error, res) {
      (q[1])(error, res);
      _next(o, m, i);
    });
  };

  var Agent = function () {
    events.EventEmitter.call(this);
  };
  util.inherits(Agent, events.EventEmitter);

  Agent.prototype._idname = function () {
    return c_heart._name;
  };

  Agent.prototype._status = function () {
    return [_free(), _status];
  };

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
      m.query(sql, null, function (error, res) {
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

  var _readonly = function (sql) {
    return sql.match(/^(SELECT|SHOW|DESC|DESCRIBE|KILL)\s+/i) ? true : false;
  };

  /**
   * @ 写请求
   */
  var w_queue = [];

  /**
   * @ 读请求
   */
  var r_queue = [];

  /**
   * @ DB连接
   */
  var _client = {};

  MysqlPool.prototype.addserver = function (config) {
    var m = createAgent(config, options);
    var k = m._idname();
    if (_client[k]) {
      return;
    }

    _client[k] = m;
    m.on('free', function (num, mode) {
      var f;
      var s = (m._status() || [])[1];

      if ((WRITABLE & s) > 0) {
        f = w_queue.shift() || r_queue.shift();
      } else if ((READONLY & s) > 0) {
        f = r_queue().shift();
      }

      if (f && f.length > 1) {
        m.query(f[0], f[1]);
      }
    });
  };

  var _find = function (flag) {
    var s = [];
    for (var i in _client) {
      s = _client[i]._status();
      if (s && s[0] && (s[1] & flag) > 0) {
        return i;
      }
    }
  };

  /* {{{ function _querywithouttimeout() */

  var _querywithouttimeout = function (sql, callback) {

    if (true !== _readonly(sql)) {
      if (w_queue.length > 0) {
        w_queue.push([sql, callback]);
        return;
      }

      var i = _find(WRITABLE);
      if (i) {
        _client[i].query(sql, callback);
      } else {
        w_queue.push([sql, callback]);
      }

      return;
    }

    if (r_queue.length > 0){
      r_queue.push([sql, callback]);
      return;
    }

    var i = _find(READONLY);
    if (i) {
      _client[i].query(sql, callback);
    } else {
      r_queue.push([sql, callback]);
    }
  };
  /* }}} */

  MysqlPool.prototype.query = function (sql, timeout, callback) {
    if ('function' === (typeof timeout)) {
      callback = timeout;
      timeout = 0;
    }

    if (!timeout || timeout < 0) {
      return _querywithouttimeout(sql, callback);
    }

    var _self = this;
    var tmout = setTimeout(function () {
      callback(QueryError('QueryTimeout', 'Mysql query timeout after ' + timeout + ' ms.'));
      callback = function (error, res) {
        _self.emit('timeout', error, res, sql);
      }
    }, timeout);
    _querywithouttimeout(sql, function (error, res) {
      clearTimeout(tmout);
      tmout = null;
      callback(error, res);
    });
  };

  return new MysqlPool();
};

