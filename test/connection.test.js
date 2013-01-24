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
    });

    var now = Date.now();
    _me.query('SELECT SLEEP(0.02)', 15, function (e, r) {
      e.should.have.property('name', 'QueryTimeout');
      e.message.should.include(getAddress(Common.config));
      (Date.now() - now).should.below(20);
      _me.close();
      done();
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
            error.should.have.property('code', 'PROTOCOL_ENQUEUE_AFTER_QUIT');
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

});

