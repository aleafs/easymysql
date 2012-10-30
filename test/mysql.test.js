/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
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

/* {{{ private function createServer() */

var FakeServer = require(__dirname + '/../node_modules/mysql/test/FakeServer.js');

var createServer = function (port, cb, onquery) {

  var _me = new FakeServer();
  _me.on('connection', function (client) {
    client.handshake();
    client.on('query', function (packet) {
      onquery(client, packet);
    });
  });
  _me.listen(port, function (error) {
    if (error) {
      throw error;
    }
    cb();
  });

  return _me;
};
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

  /* {{{ should_mysql_server_restart_works_fine() */
  xit('should_mysql_server_restart_works_fine', function (done) {

    /**
     * 只开一个连接，那么SQL一定是顺序执行
     */
    var clt = Mysql.create({'maxconnection' : 1});
    clt.addserver({'host' : 'localhost', 'port' : 33749});
    clt.query('SELECT 1', 50, function () {});
    clt.query('SELECT 2', 50, function () {});

    var num = 2;

    var onquery = function (client, packet) {
      console.log(packet);
      --num;
      if (!num) {
        done();
      } else {
        svr.destroy();
        svr = createServer(33749, function () {}, onquery);
      }
    };

    var svr = createServer(33749, function () {}, onquery);
  });
  /* }}} */

});

