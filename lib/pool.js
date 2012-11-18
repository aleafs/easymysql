/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var Emitter = require('events').EventEmitter;
var Connection = require(__dirname + '/connection.js');

var READONLY  = 1;
var WRITABLE  = 2;

var HEARTBEAT_TIMEOUT = 100;
var SLAVE_JUDGE_QUERY = 'SHOW VARIABLES LIKE "READ_ONLY"';

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
   * @ 心跳计时器
   */
  var tbeat = null;

  /* {{{ private function startup() */
  var startup = function (o) {
    var c = Connection.create(config);
    var s = 0;
    ['error', 'close'].forEach(function (i) {
      c.on(i, function (e) {
        o.emit('state', s);
        if (e) {
          o.emit('error', e);
        }
        conns.shift();
        setTimeout(function () {
          startup(o);
        }, pause);
        pause = pause + pause;
      });
    });
    (function heartbeat() {
      c.query(SLAVE_JUDGE_QUERY, HEARTBEAT_TIMEOUT, function (e, r) {
        var t = 0;
        if (e) {
          o.emit('error', e);
        } else {
          pause = 50;
          if ((((r && r.shift()) || {}).Value + '').match(/^(off)$/i)) {
            t = (READONLY | WRITABLE);
          } else {
            t = READONLY;
          }
        }

        if (s !== t) {
          s = t;
          o.emit('state', s);
        }
        tbeat = setTimeout(heartbeat, 10 * HEARTBEAT_TIMEOUT);
      });
    })();
    conns.unshift(c);
  };
  /* }}} */

  /* {{{ private function execute() */
  /**
   * execute query queue
   */
  var execute = function (o) {
    var m = 1 + _options.maxconnections + o._stack - conns.length;
    while (m && o._queue.length) {
      var f = o._queue.shift();
      var i, c;
      do {
        i = o._stack.pop();
        if (i && conns[i]) {
          c = conns[i];
          // clearTimeout
        }
      } while (i && !c);

      if (!c) {
        c = Connection.create(config);
        conns.push(c);
        ['error', 'close'].forEach(function (k) {
          c.on(k, function (e) {
            var i = conns.indexOf(c);
            if (i > -1) {
              conns = conns.splice(i, 1);
            }
          });
        });
      }

      f(c);
      m--;
    }
  };
  /* }}} */

  /* {{{ private function release() */
  /**
   *
   */
  var release = function (o, c) {
    var i = conns.indexOf(c);
    // XXX: we use conns[0] to heartbeat
    if (i > 0) {
      o._stack.push(i);
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

  return new Mysql();
};

