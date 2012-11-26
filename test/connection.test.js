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

  /* {{{ should_connnect_error_works_fine() */
  it('should_connnect_error_works_fine', function (done) {
    var _me = Connection.create({'host' : 'localhost', 'port' : 80});
    _me.on('close', function (e) {
      e.should.have.property('name', 'MysqlError');
      e.message.should.include('@localhost:80');
      _me.close();
      done();
    });
  });
  /* }}} */

  /* {{{ should_query_timeout_works_fine() */
  it('should_query_timeout_works_fine', function (done) {
    var _me = Connection.create(Common.config);
    _me.on('late', function (e, r) {
      should.ok(!e);
      r.should.includeEql({'SLEEP(0.02)' : '0'});
      _me.close();
      done();
    });

    var now = Date.now();
    _me.query('SELECT SLEEP(0.01)', 0, function (e, r) {
      should.ok(!e);
      (Date.now() - now).should.above(9);
      r.should.includeEql({'SLEEP(0.01)' : '0'});
    });

    var now = Date.now();
    _me.query('SELECT SLEEP(0.02)', 15, function (e, r) {
      e.should.have.property('name', 'QueryTimeout');
      e.message.should.include(getAddress(Common.config));
      (Date.now() - now).should.below(20);
    });
  });
  /* }}} */

  /* {{{ should_multi_connect_works_fine() */
  it('should_multi_connect_works_fine', function (done) {
    var _me = Connection.create(Common.config);
    for (var i = 0; i < 10; i++) {
      _me.connect(Common.config.timeout);
    }
    _me.on('error', function (e) {
      (true).should.eql(false);
    });
    _me.query('SHOW DATABASES', 1000, function (e, r) {
      should.ok(!e);
      r.should.includeEql({'Database' : 'mysql'});
      _me.close();
      done();
    });
  });
  /* }}} */

  /* {{{ should_connect_timeout_works_fine() */
  it('should_connect_timeout_works_fine', function (done) {
    var blocker = getBlocker(33061, function () {
      var _config = Common.extend({
        'host' : 'localhost', 'port' : 33061, 'timeout' : 50,
      });

      blocker.blocking = true;
      //blocker.block();
      var _me = Connection.create(_config);
      _me.on('error', function (e) {
        e.should.have.property('name', 'ConnectTimeout');
        e.message.should.include(getAddress(_config));
        blocker.close();
        _me.close();
        done();
      });
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
      ['error', 'close'].forEach(function (i) {
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
            'close', {
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

