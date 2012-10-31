/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var interceptor = require('interceptor');
var Mysql = require(__dirname + '/../');

var config = require(__dirname + '/common.js').extend();

var address = util.format('%s@%s:%d', config.user, config.host, config.port);

describe('mysql pool', function () {

  /* {{{ should_query_timeout_works_fine() */
  it ('should_query_timeout_works_fine', function (done) {
    var _me = Mysql.create({'maxconnection' : 1});
    _me.on('error', function (error) {
      console.log(error);
    });

    _me.on('notice', function (message) {
      message.should.eql(util.format('hearbeat for "%s" state changed to 3', address));
    });

    _me.addserver(config);
    _me.on('timeout', function (error, res, sql) {
      should.ok(!error);
      sql.should.eql('SELECT SLEEP(0.06) AS a');
      res.should.eql([{'a':'0'}]);
      done();
    });

    _me.query('SELECT 1', function (error, res) {
      should.ok(!error);
      _me.query('SELECT SLEEP(0.06) AS a', 50, function (error, res) {
        should.ok(!res);
        error.should.have.property('name', 'QueryTimeout');
      });
    });
  });
  /* }}} */

  /* {{{ should_mysql_conn_pool_works_fine() */
  it('should_mysql_conn_pool_works_fine', function (done) {

    var _me = Mysql.create({
      'maxconnection' : 2
    });
    _me.on('error', function (error) {});
    _me.on('notice', function (message) {
      //console.log(message);
    });
    _me.addserver(config);
    _me.addserver(config);  // XXX: 失效,混测试覆盖率

    /**
     * @连接不同的机器 
     */
    _me.addserver({
      'host'  : '1.1.1.1',
      'port'  : 3306,
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

});

