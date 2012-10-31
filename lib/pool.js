/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var Util = require('util');
var EventEmitter = require('events').EventEmitter;
var Connection = require(__dirname + '/connection.js');

var READONLY  = 1;
var WRITABLE  = 2;

var HEARTBEAT_TIMEOUT = 100;
var SLAVE_JUDGE_QUERY = 'SHOW VARIABLES LIKE "READ_ONLY"';

exports.create = function (options, config) {

  var _options = {
    'maxconnections'  : 4,
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
  var c_query = {};

  /**
   * @ 空闲连接
   */
  var c_stack = [];

  /**
   * @ 断开定时器
   */
  var c_timer = {};

  var MysqlPool = function () {
    EventEmitter.call(this);
    this._init();
  };
  Util.inherits(MysqlPool, EventEmitter);

  /* {{{ private prototype _init() */
  MysqlPool.prototype._init = function () {
    var _self = this;
    c_heart = Connection.create(config);
    c_heart.on('error', function (e) {
      _self.emit('error', e);
    });
    c_heart.on('close', function () {
      _self._init();
    });

    (function heartbeat() {
      c_heart.query(SLAVE_JUDGE_QUERY, HEARTBEAT_TIMEOUT, function (e, r) {
        setTimeout(heartbeat, 10 * HEARTBEAT_TIMEOUT);

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

  /* {{{ public prototype getconn() */
  /**
   * @ 获取一个连接
   *
   * @ access public
   */
  MysqlPool.prototype.getconn = function () {
  };
  /* }}} */

  /* {{{ public prototype release() */
  /**
   * @ 释放一个连接
   *
   * @ access public
   */
  MysqlPool.prototype.release = function (i) {
    if (c_query[i]) {
      c_timer[i] = setTimeout(function () {
        c_timer[i].close();
      }, _options.maxidletime);
    }
  };
  /* }}} */

  return new MysqlPool();
};

