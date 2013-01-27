/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var _COMMIT_TIMEOUT = 100;

exports.create = function (connection, pool) {

  /**
   * @ 是否开始了事务
   */
  var start = false;

  /**
   * XXX: 连接断开前自动ROLLBACK
   */
  var _me = {};

  _me.query = function (sql, timeout, callback) {
    if ('function' === (typeof timeout)) {
      callback = timeout;
      timeout = 0;
    }

    if (start) {
      connection.query(sql, timeout, callback);
      return;
    }

    connection.query('BEGIN;', 100, function (error, res) {
      if (error) {
        return callback(error, res);
      }

      start = true;
      connection.query(sql, timeout, callback);
    });
  };

  _me.commit = function (cb) {
    var after = function (e, r) {
      cb && cb(e, r);
      pool.release(connection);
    };

    if (!start) {
      after(null);
    } else {
      connection.query('COMMIT', _COMMIT_TIMEOUT, after);
    }
  };

  _me.rollback = function (cb) {
    var after = function (e, r) {
      cb && cb(e, r);
      pool.release(connection);
    };

    if (!start) {
      after(null);
    } else {
      connection.query('ROLLBACK', _COMMIT_TIMEOUT, after);
    }
  };

  return _me;
};

