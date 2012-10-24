/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var should  = require('should');
var Mysql   = require(__dirname + '/../');

/**
 * @mysql配置
 */
var options = require(__dirname + '/config.json');

describe('mysql with node-mysql', function () {

  /* {{{ should_mysql_with_4_conn_pool_works_fine() */
  it('should_mysql_with_4_conn_pool_works_fine', function (done) {
    var _me = Mysql.create(options);
    _me.query('SELECT 1', function (error, rows) {
      should.ok(!error);
      rows.should.eql([{'1':'1'}]);
      done();
    });
  });
  /* }}} */

  /* {{{ should_mysql_blackhole_works_fine() */
  it('should_mysql_blackhole_works_fine', function (done) {
    var mysql   = require(__dirname + '/../../lib/blackhole/mysql.js').create();
    mysql.query('select', function (error, data) {
      error.toString().should.include('MysqlBlackhole');
      done();
    });
  });
  /* }}} */

  /* {{{ should_mysql_query_works_fine() */
  it('should_mysql_query_works_fine', function (done) {
    options.database = 'test';
    var _me = Mysql.create(options);
    var sql = 'CREATE TABLE only_for_unittest (' + 
      'id int(10) unsigned not null auto_increment primary key,'+
      'txt varchar(2) not null default ""' +
      ')ENGINE=MYISAM';

    _me.query('DROP TABLE IF EXISTS only_for_unittest', function (error, info) {
      should.ok(!error);
      info.should.have.property('affectedRows', 0);

      _me.query(sql, function (error, info) {
        should.ok(!error);
        _me.query('INSERT INTO only_for_unittest (txt) VALUES ("te")', function (error, info) {
          info.should.have.property('insertId', 1);
          info.should.have.property('affectedRows', 1);
          _me.query('SELECT id,txt FROM test.only_for_unittest', function (error, rows) {
            rows.should.eql([{
              'id'  : 1,
              'txt' : 'te',
            }]);
            _me.query('SELECT * FROM test.only_for_unittest WHERE 0', function (error, rows) {
              should.ok(!error);
              rows.should.have.property('length', 0);
              done();
            });
          });
        });
      });
    });
  });
  /* }}} */

});

xdescribe('mysql pool', function () {

  var _pool = Mysql.createPool({
    'poolsize' : 10,
  });
  _pool.addserver(options);

  it ('should_connect_pool_works_fine', function (done) {

    var num = 1;
    for (var i = 0; i < num; i++) {
      _pool.query('SELECT SLEEP(0.1) AS v,' + i + ' AS k', {'timeout' : 20}, function (error, res) {
        console.log(error);
        if (0 === (--num)) {
          done();
        }
      });
    }
  });

  /* {{{ should_query_timeout_works_fine() */
  it('should_query_timeout_works_fine', function (done) {
    var _me = Mysql.create(options);

    _me.on('timeout', function (error, res) {
      should.ok(!error);
      res.should.eql([{'a' : '0'}]);
      _me.close(done);
    });

    _me.query('SELECT SLEEP(0.05) AS a', {'timeout' : 20}, function (error, res) {
      error.should.have.property('name', 'QueryTimeout');
      setTimeout(function () {}, 60);
    });
  });
  /* }}} */

});

