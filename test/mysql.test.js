/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var interceptor = require('interceptor');
var Mysql = require(__dirname + '/../');

/**
 * @mysql配置
 */
/* {{{ */
var config = {
  'host'  : '127.0.0.1',
  'port'  : 3306,
  'user'  : 'root',
  'password'  : ''
};
try {
  var _ = require(__dirname + '/config.json');
  for (var i in _) {
    config[i] = _[i];
  }
} catch (e) {
}
/* }}} */

describe('mysql pool', function () {

  /* {{{ should_query_timeout_works_fine() */
  it ('should_query_timeout_works_fine', function (done) {
    var _me = Mysql.create({'maxconnection' : 2});
    _me.addserver(config);

    _me.on('timeout', function (error, res, sql) {
      should.ok(!error);
      sql.should.eql('SELECT SLEEP(0.06) AS a');
      res.should.eql([{'a':'0'}]);
      done();
    });

    _me.query('SELECT SLEEP(0.06) AS a', 50, function (error, res) {
      should.ok(!res);
      error.should.have.property('name', 'QueryTimeout');
    });
  });
  /* }}} */

  /* {{{ should_mysql_conn_pool_works_fine() */
  it('should_mysql_conn_pool_works_fine', function (done) {

    var _me = Mysql.create({
      'maxconnection' : 2
    });
    _me.addserver(config);

    /**
     * @连接不同的机器 
     */
    _me.addserver({
      'host'  : '1.1.1.1',
      'user'  : 'root',
      'password'  : ''
    });

    var now = Date.now();
    var num = 5;
    for (var i = 0; i < num; i++) {
      _me.query('SELECT SLEEP(0.03) AS a', 200, function (error, rows) {
        should.ok(!error);
        rows.should.eql([{'a' : '0'}]);
        if (0 === (--num)) {
          // 30 * 5 = 150 (ms)
          (Date.now() - now).should.below(120);
          done();
        }
      });
    }
  });
  /* }}} */

  /* {{{ should_reconnect_works_fine() */
  it ('should_reconnect_works_fine', function (done) {

    var blocker = interceptor.create(util.format('%s:%d', config.host, config.port || 3306));
    blocker.listen(33061);

    var _me = Mysql.create({'maxconnection' : 1});
    _me.addserver({
      'host'  : 'localhost',
      'port'  : 33061,
      'user'  : config.user,
      'password' : config.password
    });

    blocker.block();

    _me.query('SHOW DATABASES', 25, function (error, res) {
      error.should.have.property('name', 'QueryTimeout');

      blocker.open();
      _me.query('SHOW DATABASES', 20, function (error, res) {
        console.log(error);
    //    should.ok(!error);
        done();
      });
    });
  });
  /* }}} */

});

