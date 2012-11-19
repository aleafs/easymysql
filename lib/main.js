/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var Emitter = require('events').EventEmitter;
var Pool = require(__dirname + '/pool.js');

var READONLY = 1;
var WRITABLE = 2;

exports.create = function (options) {

  /**
   * @ 心跳SQL
   */
  var hbquery = 'SHOW VARIABLES LIKE "READ_ONLY"';

  /* {{{ private function hbparse() */
  /**
   * parse heartbeat result
   *
   * @return Integer 0 : offline; 1 : readonly; 3 : writable
   */
  var hbparse = function (res) {
    if (!res || !res.length) {
      return 0;
    }

    var s = READONLY;
    if (((res.shift() || {}).Value + '').match(/^(off)$/i)) {
      s |= WRITABLE;
    }

    return s;
  };
  /* }}} */

  var Cluster = function () {
    Emitter.call(this);
  };
  util.inherits(Cluster, Emitter);

  /**
   * @ 连接池列表
   */
  var backups = {};

  /**
   * @ 读写列表
   */
  var rwlists = [];

  /**
   * @ 只读列表
   */
  var rolists = [];

  /* {{{ public prototype setHeartBeatQuery() */
  Cluster.prototype.setHeartBeatQuery = function (sql, parser) {
    Object.keys(backups).forEach(function (i) {
      backups[i].setHeartBeatQuery(sql);
    });
    hbquery = sql;
    if ('function' === (typeof parser)) {
      hbparse = parser;
    }
  };
  /* }}} */

  /* {{{ public prototype addserver() */
  Cluster.prototype.addserver = function (config) {
    var p = Pool.create(options, config);
    backups[p._name()] = p;

    p.setHeartBeatQuery(hbquery);
    p.on('state', function (res) {
      var s = hbparse(res);
    });
  };
  /* }}} */

  /* {{{ public function query() */
  Cluster.prototype.query = function (sql, tmout, callback) {
  };
  /* }}} */

  return new Cluster();
};

