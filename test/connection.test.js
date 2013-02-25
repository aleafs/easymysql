/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var interceptor = require('interceptor');

var Common = require(__dirname + '/common.js');

var Connection = require(__dirname + '/../lib/connection.js');
var getBlocker = function (port, cb) {
  var cfg = Common.extend();
  var _me = interceptor.create(util.format('%s:%d', cfg.host, cfg.port || 3306));
  _me.listen(port, cb);
  return _me;
};

var getAddress = function (config) {
  return util.format('%s@%s:%d', config.user, config.host, config.port);
};

describe('mysql connection', function () {

  it('select * from table where a =:a and b = :b and c = :c and d=:d', function (done) {
    var sql = {
      sql: 'select * from table where a =:a and b = :b and c = :c and d=:d AND e IN (:e)',
      params: {a: 1, b: 2, d: '3', e : [1.23,2,'5']}
    };
    var con = Connection.create(Common.config);
    sql = con.format(sql.sql, sql.params);
    sql.should.include("a =1");
    sql.should.include("b = 2");
    sql.should.include('c = NULL');
    sql.should.include("d='3'");
    sql.should.include("e IN (1.23, 2, '5')");
    done();
  });

  /* {{{ should_query_timeout_works_fine() */
  it('should_query_timeout_works_fine', function (done) {
    var _me = Connection.create(Common.config);

    var now = Date.now();
    _me.query('SELECT SLEEP(0.01)', 0, function (e, r) {
      should.ok(!e);
      (Date.now() - now).should.above(9);
      r.should.includeEql({'SLEEP(0.01)' : 0});

      var now2 = Date.now();
      _me.query('SELECT SLEEP(0.02)', 15, function (e, r) {
        e.should.have.property('name', 'QueryTimeout');
        e.message.should.include(getAddress(Common.config));
        (Date.now() - now2).should.below(20);
        _me.close();
        done();
      });
    });

  });

  it('should_query_timeout_info_with_right_port', function (done) {
    var config = Common.extend();
    config.port = undefined;
    var _me = Connection.create(config);
    var now = Date.now();
    _me.query('SELECT SLEEP(0.01)', 0, function (e, r) {
      should.ok(!e);
      (Date.now() - now).should.above(9);
      r.should.includeEql({'SLEEP(0.01)' : 0});
    });

    var now = Date.now();
    _me.query('SELECT SLEEP(0.02)', 15, function (e, r) {
      e.should.have.property('name', 'QueryTimeout');
      e.message.should.include(getAddress(Common.config));
      e.message.should.not.include('NaN');
      (Date.now() - now).should.below(20);
      _me.close();
      done();
    });
  });

  /* }}} */

  /* {{{ should_got_server_restart_event() */
  it('should_got_server_restart_event', function (done) {
    var blocker = getBlocker(33061, function () {
      var _config = Common.extend({
        'host' : 'localhost', 'port' : 33061
      });

      blocker.open();
      var _me = Connection.create(_config);

      var _events = [];
      ['error'].forEach(function (i) {
        _me.on(i, function (e) {
          if (e) {
            e.message.should.include(getAddress(_config));
          }
          _events.push([i].concat(Array.prototype.slice.call(arguments, 0)));
        });
      });

      _me.query('SHOW DATABASES', 100, function (error, res) {
        should.ok(!error);
        res.should.includeEql({'Database' : 'mysql'});

        var afterClosed = function () {
          _events.should.eql([[
            'error', {
              'fatal' : true, 'code' : 'PROTOCOL_CONNECTION_LOST', 'name' : 'MysqlError'}
              ]]);

          _me.query('SHOW DATABASES', 100, function (error, res) {
            should.ok(error);
            error.should.have.property('code', 'PROTOCOL_CONNECTION_LOST');
            error.message.should.include(getAddress(_config));
            _me.close();
            done();
          });

        };

        blocker.outArr[0].once('close', function () {
          setTimeout(afterClosed, 20);
        });

        /**
         * XXX: server 端关闭
         */
        _events = [];
        blocker.close();
      });
    });
  });
  /* }}} */

  it('should_connection_connected_api_works_fine', function (done) {
    var config = Common.extend();
    var _me = Connection.create(config);
    should.ok(_me.connected());
    _me.close();
    _me.connected().should.be.false;
    done();
  });

  it('should_connected_api_works_fine_when_server_blocked', function (done) {
    var blocker = getBlocker(33063, function () {
      var _config = Common.extend({
        'host' : 'localhost', 'port' : 33063
      });
      blocker.block();

      var afterBlock = function () {
        var _me = Connection.create(_config);
        _me.query('SHOW DATABASES', 0, function (e, r) {
          should.exist(e);
          e.code.should.eql('ECONNREFUSED');
          should.ok(e.fatal);
        });
        _me.query('SHOW DATABASES', 100, function (e, r) {
          should.exist(e);
          e.code.should.eql('ECONNREFUSED');
          should.ok(e.fatal);
          _me.close();
          blocker.close();
          done();
        });
      };

      setTimeout(afterBlock, 10);
    });
  });

  it('should_socket_timeout_works_fine', function (done) {
    var _config = Common.extend({
      sockettimeout : 20
    });
    var _me = Connection.create(_config);
    _me.query('SELECT SLEEP(1)', 'none', function (err, res) {
      should.exist(err);
      should.ok(err.fatal);
      // error is caused by socket timeout
      err.name.should.eql('SocketTimeout');
      _me.close();
      done();
    });

  });

  describe('a error lost connection case', function () {

    it('should_return_error_when_query_after_the_close_method_called', function (done) {
      var _me = Connection.create(Common.config);
      _me.on('error', function (e) {});
      var _count = 5;
      var errors = [];
      var _back = function (err, res) {
        if (err) {
          err.code.should.eql('PROTOCOL_ENQUEUE_AFTER_QUIT');
          errors.push(err);
        }
        _count --;
        if(_count <= 0) {
          errors.length.should.eql(3);
          done();
        }
      };

     (function x() {
        _me.query('SELECT SLEEP(0.2)', 0, function (e, r) {
          _back(e, r);
          if (3 === _count) {
            _me.close();
          }
          if (_count) {
            x();
          }
        });
     })();

    });

    it('should_socket_timeout_then_close_not_hang_up', function (done) {
      var _config = Common.extend({
        sockettimeout : 10
      });
      var _me = Connection.create(_config);

      _me.on('error', function (e) {});
      var _count = 5;
      var errors = {};
      var _back = function (err, res) {
        if (err) {
          if (errors[err.name]) {
            errors[err.name] ++;
          } else {
            errors[err.name] = 1;
          }
        }
        _count --;
        if(_count <= 0) {
          errors.MysqlError.should.eql(3);
          errors.SocketTimeout.should.eql(2);
          done();
        }
      };

      (function x() {
         _me.query('SELECT SLEEP(0.2)', 0, function (e, r) {
           _back(e, r);
           if (3 === _count) {
             _me.close();
           }
           if (_count) {
             x();
           }
         });
      })();
    });
  });
});


