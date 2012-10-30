/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var interceptor = require('interceptor');

var Common = require(__dirname + '/common.js');

var LIBPATH = process.env.MYSQL_CLUSTER_COV ? 'lib-cov' : 'lib';
var Connection = require(util.format('%s/../%s/connection.js', __dirname, LIBPATH));

describe('mysql connection', function () {

  /* {{{ should_reconnect_works_fine() */
  it('should_reconnect_works_fine', function (done) {

    var config  = Common.extend();
    var blocker = interceptor.create(util.format('%s:%d', config.host, config.port || 3306));
    blocker.listen(33061);

    blocker.block();

    var _me = Connection.create(Common.extend({
      'host' : 'localhost',
      'port' : 33061
    }));

    _me.on('error', function (e) {
      //console.log(e);
    });

    _me.query('SHOW DATABASES', 25, function (error, res) {
      error.should.have.property('name', 'QueryTimeout');
      blocker.open();

      _me.on('state', function (code) {
        if (code < 1) {
          return;
        }
        _me.query('SHOW DATABASES', 20, function (error, res) {
          should.ok(!error);
          JSON.stringify(res).should.include('{"Database":"mysql"}');
          _me.close(done);
        });
      });
    });
  });
  /* }}} */

  /* {{{ should_query_timeout_works_fine() */
  it('should_query_timeout_works_fine', function (done) {
    var _me = Connection.create(Common.config);

    _me.on('error', function (e) {
    });

    _me.on('timeout', function (error, res, sql) {
      should.ok(!error);
      sql.should.eql('SELECT SLEEP(0.02)');
      _me.close(done);
    });

    _me.query('SELECT SLEEP(0.02)', 10, function (error, res) {
      error.should.have.property('name', 'QueryTimeout');
      error.message.should.include('Mysql query timeout after 10 ms');
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

});

