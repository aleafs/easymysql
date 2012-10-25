/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var Mysql = require(__dirname + '/../');

/**
 * @mysql配置
 */
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

describe('mysql with node-mysql', function () {

  /* {{{ should_mysql_with_2_conn_pool_works_fine() */
  it('should_mysql_with_2_conn_pool_works_fine', function (done) {
    var _me = Mysql.create(config, {
      'maxconnection' : 2
    });

    _me.on('free', function (num, flag) {
      flag.should.eql(3);
    });

    var now = Date.now();
    var num = 5;
    for (var i = 0; i < num; i++) {
      _me.query('SELECT SLEEP(0.03) AS a', function (error, rows) {
        should.ok(!error);
        rows.should.eql([{'a' : '0'}]);
        if (0 === (--num)) {
          (Date.now() - now).should.below(150);
          done();
        }
      });
    }
  });
  /* }}} */

});

describe('mysql pool', function () {

  it ('should_mysql_pool_works_fine', function (done) {

    var _me = Mysql.createPool({
      'maxconnection' : 2
    });

    _me.addserver(config);
    _me.addserver(config);
    _me.addserver({
      'host'  : '1.1.1.1',
      'user'  : 'root',
      'password'  : ''
    });
    _me.query('SHOW DATABASES', 100, function (error, res) {
      should.ok(!error);
      JSON.stringify(res).should.include('{"Database":"mysql"}');
      done();
    });
  });

});

