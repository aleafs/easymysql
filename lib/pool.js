/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var Emitter = require('events').EventEmitter;

var Queue = require('safequeue');
var Connection = require(__dirname + '/connection.js');

var HEARTBEAT_TIMEOUT = 1000;

var _QueueTimeoutError = function (msg) {
  var e = new Error(msg);
  e.name = 'QueueTimeout';
  return e;
};

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
  var pause = 100;

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

  /* {{{ private function startup() */
  var startup = function (o) {

    clearTimeout(tbeat);
    tbeat = null;

    var c = Connection.create(config);
    c.on('error', function (e) {
      heartbeatER(e);
    });

    var s = '';
    conns.unshift(c);

    function heartbeat() {
      c.query(hbsql, HEARTBEAT_TIMEOUT, function (e, r) {
        if (e) {
          heartbeatER(e);
        } else {
          heartbeatOK(r);
        }
      });
    };
    heartbeat();

    function heartbeatOK(r) {
      tbeat = setTimeout(heartbeat, 10 * HEARTBEAT_TIMEOUT);
      pause = 100;
      var t = JSON.stringify(r);
      if (t !== s) {
        s = t;
        o.emit('state', r);
      }
    }

    function heartbeatER(e) {
      o.emit('state');
      conns.shift();
      setTimeout(function () {
        if (c) {
          c.close();
          c = null;
        }
        startup(o);
      }, pause);

      pause = Math.min(pause + pause, 60000);
      o.emit('error', e);
    }
  };
  /* }}} */

  /* {{{ private function _remove() */
  var _remove = function (c, o) {
    var i = conns.indexOf(c);
    if (i < 0) {
      return;
    }
    c.close();
    c = null;
    conns.splice(i, 1);
    i = o._stack.indexOf(i);
    if (i > -1) {
      o._stack.splice(i, 1);
    }
  };
  /* }}} */

  /* {{{ private function execute() */
  var execute = function (c, o, s) {
    c.query(s[0], s[1], function (e, r) {
      (s[2])(e, r);
      if (e && e.fatal) {
        _remove(c, o);
        return;
      }

      s = o._queue.shift();
      if (!s) {
        release(o, c);
      } else {
        execute(c, o, s);
      }
    });
  };
  /* }}} */

  /* {{{ private function _wakeup() */
  /**
   * wake up a pool to execute query
   */
  var _wakeup = function (o) {
    var m = Math.min(_options.maxconnections, 1 + _options.maxconnections + o._stack.length - conns.length);
    while (m && o._queue.size()) {
      var s = o._queue.shift();
      var i, c;
      do {
        i = o._stack.pop();
        if (i && conns[i]) {
          c = conns[i];
          clearTimeout(timer[i]);
          timer[i] = null;
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
      execute(c, o, s);
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
    var _name = this._name();
    this._queue = Queue.create({'timeout' : 0, 'maxitem' : 0});
    this._queue.on('timeout', function (item, tmout, pos) {
      (item[2])(_QueueTimeoutError(util.format(
            'Query stays in the queue more than %d ms (%s)', tmout, _name)));
    });

    var _self = this;
    this._queue.on('fill', function () {
      _wakeup(_self);
    });

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
  Mysql.prototype.query = function (sql, tmout, cb) {
    this._queue.push([sql, tmout, cb], tmout);
    var n = this._queue.size();
    if (n > 0) {
      this.emit('busy', n, _options.maxconnections);
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

