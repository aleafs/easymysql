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

  /* {{{ private function startup() */
  var startup = function (o) {

    clearTimeout(tbeat);
    tbeat = null;

    var c = Connection.create(config);
    c.on('error', function () {});

    var s = '';
    o._conns.unshift(c);

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
      o._conns.shift();
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
    var i;
    i = o._conns.indexOf(c);
    if (i > 0) {
      o._conns.splice(i, 1);
    }
    i = o._stack.indexOf(c);
    if (i >= 0) {
      o._stack.splice(i, 1);
    }

    clearTimeout(c.timer);
    c.timer = null;
    c.close();
    c = null;

    _wakeup(o);
  };
  /* }}} */

  /* {{{ private function execute() */
  var execute = function (c, o, s) {
    c.query(s[0], s[1], function (e, r) {
      (s[2])(e, r);
      if (e && e.fatal) {
        return _remove(c, o);
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

    var m = Math.min(_options.maxconnections, 1 + _options.maxconnections + o._stack.length - o._conns.length);
    while (m && o._queue.size()) {
      var s = o._queue.shift();
      var i, c;
      do {
        c = o._stack.pop();
        if (c && c.timer) {
          clearTimeout(c.timer);
          c.timer = null;
        }
      } while (o._stack.length > 0 && !c);

      if (!c || !c.connected()) {
        c = Connection.create(config);

        ['error'].forEach(function (k) {
          c.once(k, function (e) {
            o.emit('error', e);
            _remove(c, o);
          });
        });

        o._conns.push(c);
      }

      execute(c, o, s);
      m--;
    }
  };
  /* }}} */

  /* {{{ private function release() */
  var release = function (o, c) {
    var i = o._conns.indexOf(c);
    // XXX: we use conns[0] to heartbeat
    if (i > 0) {
      o._stack.push(c);
      c.timer = setTimeout(function () {
        _remove(c, o);
      }, _options.maxidletime);
    }
  };
  /* }}} */

  var Mysql = function () {

    Emitter.call(this);

    /**
     * @ 空闲连接
     */
    this._stack = [];

    /**
     * @ 连接数组
     */
    this._conns = [];

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
  };
  util.inherits(Mysql, Emitter);

  /* {{{ public prototype _name() */
  Mysql.prototype._name = function () {
    return this._conns[0]._name;
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

