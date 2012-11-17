/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var Util = require('util');
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
   * @ 心跳连接
   */
  var c_heart = null;

  /**
   * @ 心跳状态
   */
  var c_state = 0;

  /**
   * @ 工作连接
   */
  var _worker = [];

  /**
   * @ 空闲连接
   */
  var c_stack = [];

  /**
   * @ 断开定时器
   */
  var c_timer = {};

  var MysqlPool = function () {
    Emitter.call(this);
    this._init();
  };
  Util.inherits(MysqlPool, Emitter);

  /* {{{ private prototype _init() */
  MysqlPool.prototype._init = function () {
    var _self = this;
    c_heart = Connection.create(config);
    c_heart.on('error', function (e) {
      process.nextTick(function () {
        _self.emit('error', e);
      });
    });
    c_heart.on('close', function (e) {
      // XXX: 延迟重连
      _self._init();
    });

    (function heartbeat() {
      c_heart.query(SLAVE_JUDGE_QUERY, HEARTBEAT_TIMEOUT, function (e, r) {
        setTimeout(heartbeat, 10 * HEARTBEAT_TIMEOUT);
        if (e) {
          _self.emit('error', e);
        }
        var s = e ? 0 : READONLY;
        if (((r && r.shift() || {}).Value + '').match(/^(off)$/i)) {
          s |= WRITABLE;
        }

        if (s !== c_state) {
          c_state = s;
          _self.emit('state', c_state);
        }
      });
    })();
  };
  /* }}} */

  /**
   * @ 工作队列
   */
  var callbacks = [];
  var start = function () {
    var num = c_stack.length + _options.maxconnections - _worker.length;
    while (num && callbacks.length) {
      var cb = callbacks.shift();
      var id = c_stack.pop();
      if (!id) {
        var c = c_heart.clone();
        id = _worker.push(c);
      }
      cb(_worker[id - 1], id - 1);
      num--;
    }
  };

  var release = function (i) {
    if (!_worker[i]) {
      return;
    }
    c_stack.push(i);
    c_timer[i] = setTimeout(function () {
      _worker[i].close(function () {
        delete _worker[i];
      });
    }, _options.maxidletime);
  };

  /* {{{ public prototype query() */
  /**
   * @ 执行query
   *
   * @param {Object|String} sql
   * @param {Function} callback
   */
  MysqlPool.prototype.query = function (sql, callback) {
    var n = callbacks.push(function (con, i) {
      con.query(sql, -1, function (e, r) {
        callback(e, r);
        // XXX: error.fatal ?
        if (!callbacks.length) {
          release(i);
        } else {
          (callbacks.shift())(con, i);
        }
      });
    });
    if (1 === n) {
      start();
    }
  };
  /* }}} */

  return new MysqlPool();
};

