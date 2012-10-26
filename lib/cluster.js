/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var mysql = require('mysql');
var EventEmitter = require('events').EventEmitter;

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

  EventEmitter.call(this);

  this._flag = 0;
  this._conn = mysql.createConnection(options);
  this._name = util.format('%s@%s:%d', this._conn.config.user,
      this._conn.config.host, this._conn.config.port);

  this.connect();
};
util.inherits(Connection, EventEmitter);

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

exports.create = function (options) {

  var _options = {
    'maxconnection' : 4,        /**<  单机允许的最大连接数  */
    'maxqueuedsql'  : 1000,     /**<  允许排队的最大SQL数   */
  };
  for (var i in options) {
    _options[i] = options[i];
  }

  /**
   * @ 心跳连接
   */
  var c_heart = {};

  /**
   * @ 写请求
   */
  var w_queue = [];

  /**
   * @ 读请求
   */
  var r_queue = [];

  /**
   * @ 工作连接
   */
  var c_query = [];

  /**
   * @ 空闲写连接
   */
  var w_stack = [];

  /**
   * @ 空闲读连接
   */
  var r_stack = [];

  var Cluster = function () {
    EventEmitter.call(this);
    this.on('status', function (name, mode) {

      var n = 0;
      c_query.forEach(function (c, i) {
        if (c._name === name) {
          c_query[i]._status = mode;
          n++;
        }
      });

      c_heart[name].n = n;
      if (!(mode & READONLY)) {
        return;
      }

      var h = c_heart[name];    /**<  心跳对象  */
      var c, i;

      while (h.n < _options.maxconnection && (r_queue.length || (w_queue.length && (mode & WRITABLE)))) {
        c = h.c.clone();
        c._status = mode;

        i = c_query.push(c) - 1;
        w_stack.push(i);
        h.n++;

        _nextsql(c, i);
      }

    });
  };
  util.inherits(Cluster, EventEmitter);

  /* {{{ public prototype addserver() */
  Cluster.prototype.addserver = function (config) {
    var m = new Connection(config);
    var i = m._name;
    if (c_heart[i]) {
      m.close();
      return this;
    }

    c_heart[i] = {
      'n' : 0,    /**<  连接数  */
      's' : -1,   /**<  状态    */
      'c' : m,    /**<  心跳    */
    };

    m.on('close', function () {
      m.connect();
    });

    var _self = this;
    (function heartbeat () {
      m.query('SHOW VARIABLES LIKE "read_only"', 100, function (e, r) {
        var s = e ? 0 : READONLY;
        if (((r && r.shift() || {}).Value + '').match(/^(off)$/i)) {
          s |= WRITABLE;
        }
        if (s !== c_heart[i].s) {
          _self.emit('status', i, s);
        }
        c_heart[i].s = s;
        setTimeout(heartbeat, 1000);
      });
    })();

    return _self;
  };
  /* }}} */

  var _readonly = function (sql) {
    return sql.match(/^(SELECT|SHOW|DESC|DESCRIBE|KILL)\s+/i) ? true : false;
  };

  /* {{{ private function _nextsql() */
  var _nextsql  = function (m, i) {
    var q;

    if ((WRITABLE & m._status) > 0) {
      q = w_queue.shift() || r_queue.shift();
      if (!q) {
        w_stack.push(i);
      }
    } else {
      q = r_queue.shift();
      if (!q) {
        r_stack.push(i);
      }
    }

    if (!q) {
      return;
    }

    m.query(q[0], null, function (error, res) {
      (q[1])(error, res);
      _nextsql(m, i);
    });
  };
  /* }}} */

  /* {{{ private function _getwconn() */
  var _getwconn = function () {
    var i;
    do {
      i = w_stack.pop();
      if (i && c_query[i]) {
        if (c_query[i]._status & WRITABLE) {
          return i;
        }
        r_stack.push(i);
      }
    } while (i);

    return -1;
  };
  /* }}} */

  /* {{{ private function _getrconn() */
  var _getrconn = function () {
    var i;
    do {
      i = r_stack.pop();
      if (i && c_query[i]) {
        return i;
      }
    } while (i);

    return -1;
  };
  /* }}} */

  /* {{{ private function _querywithouttimeout() */
  var _querywithouttimeout = function (sql, callback) {

    var w = _readonly(sql) ? false : true;
    var i = _getwconn();
    if (i < 0 && !w) {
      i = _getrconn();
    }

    if (i > -1) {
      c_query[i].query(sql, null, function (error, res) {
        callback(error, res);
        _nextsql(c_query[i], i);
      });
    } else if (w) {
      w_queue.push([sql, callback]);
    } else {
      r_queue.push([sql, callback]);
    }
  };
  /* }}} */

  /* {{{ public prototype query() */
  Cluster.prototype.query = function (sql, timeout, callback) {
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
  /* }}} */

  return new Cluster();

};

