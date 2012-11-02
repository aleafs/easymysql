/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var interceptor = require('interceptor');

var Common = require(__dirname + '/common.js');

var LIBPATH = process.env.MYSQL_CLUSTER_COV ? 'lib-cov' : 'lib';
var Connection = require(util.format('%s/../%s/connection.js', __dirname, LIBPATH));

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

  /* {{{ should_reconnect_works_fine() */
  it('should_reconnect_works_fine', function (done) {

    var blocker = getBlocker(33061);

    blocker.block();

    var config  = Common.extend({
      'host' : 'localhost',
        'port' : 33061
    });
    var _me = Connection.create(config);
    _me.on('error', function (e) {
      e.message.should.include(getAddress(config));
    });

    _me.query('SHOW DATABASES', 25, function (error, res) {
      error.should.have.property('name', 'QueryTimeout');
      error.message.should.include(getAddress(config));
      blocker.open();

      _me.on('state', function (code) {
        if (code < 1) {
          return;
        }
        _me.query('SHOW DATABASES', 20, function (error, res) {
          should.ok(!error);
          JSON.stringify(res).should.include('{"Database":"mysql"}');
          _me.close(function () {
            blocker.close();
            done();
          });
        });
      });
    });
  });
  /* }}} */

  /* {{{ should_mysql_restart_wroks_fine() */
  it('should_mysql_restart_wroks_fine', function (done) {
    var blocker = getBlocker(33061);

    var config = Common.extend({
      'host' : 'localhost',
        'port' : 33061
    });
    var _me = Connection.create(config);
    _me.on('error', function (e) {
      e.message.should.include(getAddress(config));
    });

    var state = 1;
    _me.on('state', function (mode) {
      mode.should.eql(state);
    });

    _me.query('SHOW ENGINES', 10, function (error, res) {
      should.ok(!error);

      blocker.close();
      state = 0;

      var expect = JSON.stringify(res);
      setTimeout(function () {
        _me.query('SHOW ENGINES', 10, function (error, res) {
          error.should.have.property('name', 'QueryTimeout');

          blocker = getBlocker(33061);
          state = 1;

          setTimeout(function () {
            _me.query('SHOW ENGINES', 1000, function (error, res) {
              should.ok(!error);
              JSON.stringify(res).should.eql(expect);
              _me.close(function () {
                blocker.close();
                done();
              });
            });
          }, 20);
        });

      }, 20);
    });
  });
  /* }}} */

  /* {{{ should_query_timeout_works_fine() */
  it('should_query_timeout_works_fine', function (done) {
    var _me = Connection.create(Common.config);

    _me.on('error', function (e) {
      e.message.should.include(getAddress(Common.config));
    });

    _me.on('timeout', function (error, res, sql) {
      _me.connect();    /**<  没有实际意义，纯粹为了覆盖率  */
      should.ok(!error);
      sql.should.eql('SELECT SLEEP(0.02)');
      _me.close(done);
    });

    _me.query('SELECT SLEEP(0.02)', 10, function (error, res) {
      error.should.have.property('name', 'QueryTimeout');
      error.message.should.include('Mysql query timeout after 10 ms');
      error.message.should.include(getAddress(Common.config));
    });
  });
  /* }}} */

  /* {{{ should_auth_fail_works_fine() */
  it('should_auth_fail_works_fine', function (done) {

    if (Common.istravis) {
      return done();
    }

    var _me = Connection.create(Common.extend({'user' : 'i_am_not_exists'}));

    var err = 0;
    _me.on('error', function (e) {
      err++;
    });

    _me.query('SHOW DATABASES', 100, function (error, res) {
      error.should.have.property('code', 'ER_ACCESS_DENIED_ERROR');
      setTimeout(function () {
        err.should.eql(2);
        _me.close(done);
      }, 100);
    });
  });
  /* }}} */

  /* {{{ should_error_message_contains_config() */
  it('should_error_message_contains_config', function (done) {
    var _me = Connection.create(Common.config);
    _me.on('error', function (e) {
      e.message.should.include(getAddress(Common.config));
    });
    _me.query('SELECT I_AM_NOT_DEFINED(123)', 100, function (error, res) {
      error.should.have.property('name', 'MysqlError');
      error.message.should.include(getAddress(Common.config));
      _me.close(done);
    });
  });
  /* }}} */

  /* {{{ should_got_exception_when_error_mysql_config() */
  it('should_got_exception_when_error_mysql_config', function (done) {
    var _me = Connection.create({'host' : '##mysql.default.host##'});
    _me.on('error', function (e) {
      e.message.should.include('getaddrinfo ENOENT');
    });
    _me.query('SHOW DATABASES', 10, function (e, res) {
      _me.close(done);
    });
  });
  /* }}} */

  xit('should_fake_server_works_fine', function (done) {
    var _me = Connection.create({'host' : 'localhost', 'port' : 33751});
    var _my = Common.liteServer(33751, function (close) {
      _me.query('SHOW VARIABLES LIKE "read_only"', 20, function (e, res) {
        console.log(e);
        console.log(res);
        close();
        _me.close(done);
      });
    });
  });
});

