/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var mysql = require('mysql');
var EventEmitter = require('events').EventEmitter;
var Connection  = require(__dirname + '/connection.js');

var READONLY  = 1;
var WRITABLE  = 2;

var SLAVE_JUDGE_SQL = 'SHOW VARIABLES LIKE "READ_ONLY"';
var _readonly = function (sql) {
  return sql.match(/^(SELECT|SHOW|DESC|DESCRIBE|KILL)\s+/i) ? true : false;
};

var QueryError = function (name, msg) {
  var e = new Error(msg || name);
  e.name = name;
  return e;
};

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

  /**
   * @ 写请求
   */
  var w_queue = [];

  /**
   * @ 读请求
   */
  var r_queue = [];

  /* {{{ private function _create() */
  var _create = function (s) {
    var m, c;
    for (var i in c_heart) {
      m = c_heart[i];
      if (m.n < _options.maxconnection && (s & m.s)) {
        c = m.c.clone();
        c.on('error', function (e) {
        });
        c._status = m.s;
        c_heart[i].n++;

        return c;
      }
    }
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

    /**
     * XXX: 实质上连接还没有建立好
     */
    var m = _create(WRITABLE);
    if (!m) {
      return -1;
    }

    return c_query.push(m) - 1;
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

    var m = _create(READONLY);
    if (!m) {
      return -1;
    }

    return c_query.push(m) - 1;
  };
  /* }}} */

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

  /* {{{ private function _querywithouttimeout() */
  var _querywithouttimeout = function (sql, callback) {

    if (w_queue.length > 0) {
      w_queue.push([sql, callback]);
      return;
    }

    var i = _getwconn();
    if (i < 0 && _readonly(sql)) {
      i = _getrconn();
    }

    if (i < 0) {
      w_queue.push([sql, callback]);
    } else {
      c_query[i].query(sql, null, function (error, res) {
        callback(error, res);
        _nextsql(c_query[i], i);
      });
    }
  };
  /* }}} */

  /* {{{ private function _check_queued_query() */
  var _check_queued_query = function () {
    var i = -1;
    while (w_queue.length > 0 || r_queue.length > 0) {
      i = _getwconn();
      if (i < 0) {
        break;
      }
      _nextsql(c_query[i], i);
    }

    while (r_queue.length > 0) {
      i = _getrconn();
      if (i < 0) {
        break;
      }
      _nextsql(c_query[i], i);
    }
  };
  /* }}} */

  var Cluster = function () {
    EventEmitter.call(this);
  };
  util.inherits(Cluster, EventEmitter);

  /* {{{ public prototype addserver() */
  Cluster.prototype.addserver = function (config) {
    var m = Connection.create(config);
    var i = m._name;

    var _self = this;
    m.on('error', function (e) {
      _self.emit('error', e);
    });

    if (c_heart[i]) {
      m.close();
      return _self;
    }

    c_heart[i] = {
      'n' : 0,    /**<  连接数  */
      's' : 0,    /**<  状态    */
      'c' : m,    /**<  心跳    */
    };

    m.on('close', function () {
      delete c_heart[i];
      _self.addserver(config);
      _self.emit('notice', util.format('hearbeat for "%s" closed, try to reconnect', i));
    });

    (function heartbeat () {
      m.query(SLAVE_JUDGE_SQL, 100, function (e, r) {
        var s = e ? 0 : READONLY;
        if (((r && r.shift() || {}).Value + '').match(/^(off)$/i)) {
          s |= WRITABLE;
        }

        var n = 0;
        c_query.forEach(function (c, i) {
          if (c._name === m._name) {
            c_query[i]._status = s;
            n++;
          }
        });
        c_heart[i].n = n;

        if (s !== c_heart[i].s) {
          _self.emit('notice', util.format('hearbeat for "%s" state changed to %d', i, s));
        }
        c_heart[i].s = s;
        if ((s & READONLY) > 0) {
          _check_queued_query();
        }

        setTimeout(heartbeat, 1000);
      });
    })();

    return _self;
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

