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

    _me.on('free', function (num) {
      console.log('a');
      done();
    });

    for (var i = 0; i < 3; i++) {
      _me.query('SELECT SLEEP(0.01) AS a', function (error, rows) {
        should.ok(!error);
        rows.should.eql([{'a' : '0'}]);
      });
    }
  });
  /* }}} */

});

