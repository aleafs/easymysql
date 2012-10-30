/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var _COMMIT_TIMEOUT = 100;

exports.create = function (connection) {

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

    connection.query('BEGIN; SET AUTOCOMMIT = 0', 100, function (error, res) {
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
      connection.close(function () {
        connection = null;
        start = null;
      });
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
      connection.close(function () {
        connection = null;
        start = null;
      });
    };

    if (!start) {
      after(null);
    } else {
      connection.query('ROLLBACK', _COMMIT_TIMEOUT, after);
    }
  };

  return _me;
};

