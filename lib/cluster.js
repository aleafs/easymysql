/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var util = require('util');
var Emitter = require('events').EventEmitter;
var Pool = require(__dirname + '/pool.js');
var Queue = require('safequeue');

var MAX_QUEUED_SIZE = 1000;

var READONLY = 1;
var WRITABLE = 2;

var _QueueTimeoutError = function (msg) {
  var e = new Error(msg);
  e.name = 'QueueTimeout';
  return e;
};

var _QueueIsFullError = function (msg) {
  var e = new Error(msg);
  e.name = 'QueueIsFull';
  return e;
};

/* {{{ private function _readonly() */
var _readonly = function (sql) {
  return sql.match(/^(SELECT|SHOW|DESC|DESCRIBE|KILL)\s+/i) ? true : false;
};
/* }}} */

/* {{{ private function _remove() */
var _remove = function (a, o) {
  var i = a.indexOf(o);
  if (i > -1) {
    a.splice(i, 1);
  }

  return a;
};
/* }}} */

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
   * @ 读写队列
   */
  var rwqueue = Queue.create({'timeout' : 0, 'maxitem' : MAX_QUEUED_SIZE});
  rwqueue.on('timeout', function (item, timeout) {
    (item[2])(_QueueTimeoutError('Query stays in the queue more than ' + timeout + ' ms'));
  });

  /**
   * @ 只读列表
   */
  var rolists = [];

  /**
   * @ 只读队列
   */
  var roqueue = Queue.create({'timeout' : 0, 'maxitem' : MAX_QUEUED_SIZE});
  roqueue.on('timeout', function (item, timeout) {
    (item[2])(_QueueTimeoutError('Query stays in the queue more than ' + timeout + ' ms'));
  });

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

  /* {{{ private function checkQueue() */
  var checkQueue = function (queue, pool, max) {
    var max = (~~max) || 4;
    while (max > 0) {
      var s = queue.shift();
      if (!s || !s.length) {
        return;
      }
      pool.query(s[0], s[1], s[2]);
      max--;
    }

    if (queue.size() > 0) {
      process.nextTick(function () {
        checkQueue(queue, pool, max);
      });
    }
  };
  /* }}} */

  /* {{{ public prototype addserver() */
  Cluster.prototype.addserver = function (config) {
    var p = Pool.create(options, config);
    var i = p._name();
    backups[i] = p;

    var _self = this;
    p.on('error', function (e) {
      _self.emit('error', e);
    });
    p.on('busy', function (n, c) {
      _self.emit('busy', n, c, i);
    });

    p.setHeartBeatQuery(hbquery);
    p.on('state', function (res) {
      var s = hbparse(res);
      if (!s) {
        rolists = _remove(rolists, i);
        rwlists = _remove(rwlists, i);
        return;
      }

      if (rolists.indexOf(i) < 0) {
        checkQueue(roqueue, p);
        rolists.push(i);
      }
      if ((WRITABLE & s) && rwlists.indexOf(i) < 0) {
        checkQueue(rwqueue, p);
        rwlists.push(i);
      }
    });
  };
  /* }}} */

  /**
   * @ 读计数器
   */
  var rocount = 0;

  /**
   * @ 写计数器
   */
  var rwcount = 0;

  /* {{{ public function query() */
  Cluster.prototype.query = function (sql, tmout, callback) {

    if ('function' === (typeof tmout)) {
      callback = tmout;
      tmout = 0;
    };

    if (!_readonly(sql.sql || sql)) {
      if (rwlists.length < 1) {
        if (rwqueue.push([sql, tmout, callback], tmout) < 0) {
          callback(_QueueIsFullError('Too many queries queued'));
        }
      } else {
        backups[rwlists[(++rwcount) % rwlists.length]].query(sql, tmout, callback);
      }
    } else {
      if (rolists.length < 1) {
        if (roqueue.push([sql, tmout, callback], tmout) < 0) {
          callback(_QueueIsFullError('Too many queries queued'));
        }
      } else {
        backups[rolists[(++rocount) % rolists.length]].query(sql, tmout, callback);
      }
    }
  };
  /* }}} */

  return new Cluster();
};

