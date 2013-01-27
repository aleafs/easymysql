/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should  = require('should');
var rewire  = require('rewire');
var Common  = require(__dirname + '/common.js');
var Cluster = rewire(__dirname + '/../lib/cluster.js');

describe('mysql cluster', function () {

  var _me;

  /*{{{ beforeEach() */
  beforeEach(function (done) {
    Cluster.__set__({
      'MAX_QUEUED_SIZE' : 5,
    });
    _me = Cluster.create({'maxconnections' : 3});
    _me.addserver(Common.config);
    _me.query('DROP DATABASE easymysql_test', 300, function (e, r) {
      _me.query('CREATE DATABASE easymysql_test', 300, function (e, r) {
        var sql = 
          'CREATE TABLE IF NOT EXISTS easymysql_test.test_table(' +
          'id int(10) unsigned not null auto_increment,' +
          'num int(10) unsigned not null default 0,' +
          'PRIMARY KEY (id)' +
          ') ENGINE=InnoDB DEFAULT CHARSET=UTF8';
        _me.query(sql, 3000, function (e, r) {
          done();
        });
      });
    });
  });
  /*}}}*/

  /* {{{ should_mysql_transact_commit_works_fine() */
  it('should_mysql_transact_commit_works_fine', function (done) {
    var tran = _me.startTransact();
    //insert one line
    tran.query('INSERT INTO easymysql_test.test_table (num) VALUES (1)', 300, function (e, r) {
      should.ok(!e);
      //insert another line
      tran.query('INSERT INTO easymysql_test.test_table (num) VALUES (2)', 300, function (e, r) {
        should.ok(!e);
        //commit
        tran.commit(function (e, r) {
          _me.query('SELECT * FROM easymysql_test.test_table', 300, function (e, r) {
            should.ok(!e);
            r.length.should.eql(2);
            done();
          });
        });
      });
    });
  });
  /* }}} */

  /* {{{ should_mysql_transact_rollback_works_fine() */
  it('should_mysql_transact_commit_works_fine', function (done) {
    var tran = _me.startTransact();
    //insert one line
    tran.query('INSERT INTO easymysql_test.test_table (num) VALUES (3)', 300, function (e, r) {
      should.ok(!e);
      //insert another line
      tran.query('INSERT INTO easymysql_test.test_table (num) VALUES (4)', 300, function (e, r) {
        should.ok(!e);
        //commit
        tran.rollback(function (e, r) {
          _me.query('SELECT * FROM easymysql_test.test_table', 300, function (e, r) {
            should.ok(!e);
            r.length.should.eql(0);
            done();
          });
        });
      });
    });
  });
  /* }}} */

});

