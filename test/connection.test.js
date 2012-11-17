/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var interceptor = require('interceptor');

var Common = require(__dirname + '/common.js');

var Connection = require(__dirname + '/../lib/connection.js');
var getBlocker = function (port) {
  var cfg = Common.extend();
  var _me = interceptor.create(util.format('%s:%d', cfg.host, cfg.port || 3306));
  _me.listen(port);
  return _me;
};

var getAddress = function (config) {
  return util.format('%s@%s:%d', config.user, config.host, config.port);
};

describe('mysql connection', function () {

  /* {{{ should_got_server_restart_event() */
  it('should_got_server_restart_event', function (done) {

    var blocker = getBlocker(33061);
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

      /**
       * XXX: server 端关闭
       */
      blocker.close();

      setTimeout(function () {
        _events.should.eql([['close']]);
        _me.query('SHOW DATABASES', 100, function (error, res) {
          console.log(error);
          should.ok(error);
          error.should.have.property('code', 'PROTOCOL_CONNECTION_LOST');
          error.message.should.include(getAddress(_config));
          _me.close(done);
        });
      }, 20);
    });
  });
  /* }}} */

  /* {{{ should_connect_timeout_works_fine() */
  it('should_connect_timeout_works_fine', function (done) {
    var blocker = getBlocker(33062);
    var _config = Common.extend({
      'host' : 'localhost', 'port' : 33062
    });

    blocker.block();
    var _me = Connection.create(_config);
    _me.on('error', function (e) {
      e.should.have.property('name');
      e.message.should.include(getAddress(_config));
      blocker.close();
      _me.close(done);
    });
  });
  /* }}} */

  /* {{{ should_query_timeout_works_fine() */
  it('should_query_timeout_works_fine', function (done) {
    var _me = Connection.create(Common.config);
    _me.on('late', function (e, r) {
      should.ok(!e);
      r.should.includeEql({'SLEEP(0.02)' : '0'});
      _me.close(done);
    });

    var now = Date.now();
    _me.query('SELECT SLEEP(0.01)', 0, function (e, r) {
      should.ok(!e);
      (Date.now() - now).should.above(9);
      r.should.includeEql({'SLEEP(0.01)' : '0'});

      _me.connect();
    });

    var now = Date.now();
    _me.query('SELECT SLEEP(0.02)', 15, function (e, r) {
      e.should.have.property('name', 'QueryTimeout');
      e.message.should.include(getAddress(Common.config));
      (Date.now() - now).should.below(20);
    });
  });
  /* }}} */

  /* {{{ should_clone_works_fine() */
  it('should_clone_works_fine', function (done) {
    var _me = Connection.create(Common.config);
    _me.query('SELECT 1', 0, function (e, r) {
      should.ok(!e);
      _me.close(function () {
        _me.close();
      });

      var the = _me.clone();
      the.query('SELECT 1', 0, function (e, r) {
        should.ok(!e);
        the.close(done);
      });
    });
  });
  /* }}} */

});

