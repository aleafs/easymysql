/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var Emitter = require('events').EventEmitter;
var Connection = require(__dirname + '/connection.js');

var HEARTBEAT_TIMEOUT = 100;

exports.create = function (options, config) {

  var _options = {
    'maxconnections' : 4,
    'maxidletime' : 60000,
  };
  for (var i in options) {
    _options[i] = options[i];
  }

  /**
   * @ 连接数组
   */
  var conns = [];

  /**
   * @ 重连暂停
   */
  var pause = 50;

  /**
   * @ 心跳SQL
   */
  var hbsql = 'SHOW VARIABLES LIKE "READ_ONLY"';

  /**
   * @ 心跳计时器
   */
  var tbeat = null;

  /**
   * @ 空闲计时器
   */
  var timer = {};

  /* {{{ private function _reboot() */
  /**
   * @ 重启心跳连接
   */
  var _reboot = function (o) {
    var c = conns.shift();
    if (!c) {
      return;
    }
    o.emit('state');
    setTimeout(function () {
      c.removeAllListeners();
      startup(o);
    }, pause);
    pause = pause + pause;
  };
  /* }}} */

  /* {{{ private function startup() */
  var startup = function (o) {
    var c = Connection.create(config);
    var s = '';
    conns.unshift(c);
    ['error', 'close'].forEach(function (i) {
      c.once(i, function (e) {
        _reboot(o);
        if (e) {
          o.emit('error', e);
        }
      });
    });
    (function heartbeat() {
      c.query(hbsql, HEARTBEAT_TIMEOUT, function (e, r) {
        tbeat = setTimeout(heartbeat, 10 * HEARTBEAT_TIMEOUT);
        if (e) {
          _reboot(o);
          o.emit('error', e);
          return;
        }

        pause = 50;
        var t = JSON.stringify(r);
        if (t !== s) {
          s = t;
          o.emit('state', r);
        }
      });
    })();
  };
  /* }}} */

  /* {{{ private function _remove() */
  var _remove = function (c, o) {
    var i = conns.indexOf(c);
    if (i < 0) {
      return;
    }
    c.close();
    conns.splice(i, 1);
    i = o._stack.indexOf(i);
    if (i > -1) {
      o._stack.splice(i, 1);
    }
  };
  /* }}} */

  /* {{{ private function execute() */
  /**
   * execute query queue
   */
  var execute = function (o) {
    var m = 1 + _options.maxconnections + o._stack.length - conns.length;
    while (m && o._queue.length) {
      var f = o._queue.shift();
      var i, c;
      do {
        i = o._stack.pop();
        if (i && conns[i]) {
          c = conns[i];
          if (timer[i]) {
            clearTimeout(timer[i]);
            delete timer[i];
          }
        }
      } while (i && !c);

      if (!c) {
        c = Connection.create(config);
        conns.push(c);
        ['error', 'close'].forEach(function (k) {
          c.once(k, function (e) {
            _remove(c, o);
          });
        });
      }

      f(c);
      m--;
    }
  };
  /* }}} */

  /* {{{ private function release() */
  var release = function (o, c) {
    var i = conns.indexOf(c);
    // XXX: we use conns[0] to heartbeat
    if (i > 0) {
      o._stack.push(i);
      timer[i] = setTimeout(function () {
        _remove(c, o);
      }, _options.maxidletime);
    }
  };
  /* }}} */

  var Mysql = function () {

    Emitter.call(this);
    startup(this);

    /**
     * @ 执行队列
     */
    this._queue = [];

    /**
     * @ 空闲连接
     */
    this._stack = [];

  };
  util.inherits(Mysql, Emitter);

  /* {{{ public prototype _name() */
  Mysql.prototype._name = function () {
    return conns[0]._name;
  };
  /* }}} */

  /* {{{ public prototype query() */
  /**
   * Get one connection and run sql
   *
   * @ param {String|Object} sql
   * @ param {Function} cb
   */
  Mysql.prototype.query = function (sql, cb) {

    var _self = this;
    var _size = this._queue.push(function (con) {
      con.query(sql, -1, function (e, r) {
        cb(e, r);
        if (e && e.fatal) {
          con.close();
          return;
        }

        if (!_self._queue.length) {
          release(_self, con);
        } else {
          (_self._queue.shift())(con);
        }
      });
    });

    if (1 === _size) {
      execute(_self);
    }
  };
  /* }}} */

  /* {{{ public prototype setHeartBeatQuery() */
  Mysql.prototype.setHeartBeatQuery = function (sql) {
    hbsql = sql;
  };
  /* }}} */

  return new Mysql();
};

