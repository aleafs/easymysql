/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should  = require('should');
var rewire  = require('rewire');
var Common  = require(__dirname + '/common.js');
var Cluster = rewire(__dirname + '/../lib/cluster.js');
var interceptor = require('interceptor');

var getBlocker = function (port, cb) {
  var cfg = Common.extend();
  var _me = interceptor.create(util.format('%s:%d', cfg.host, cfg.port || 3306));
  _me.listen(port, cb);
  return _me;
};

describe('mysql cluster', function () {

  beforeEach(function () {
    Cluster.__set__({
      'MAX_QUEUED_SIZE' : 5,
    });
  });

  /* {{{ should_mysql_cluster_works_fine() */
  it('should_mysql_cluster_works_fine', function (done) {
    var _me = Cluster.create({'maxconnections' : 1});
    _me.addserver(Common.config);

    var _messages = [];
    _me.on('busy', function (n, m) {
      _messages.push(['busy'].concat(Array.prototype.slice.call(arguments)));
    });
    _me.query('SHOW DATABASES', function (e, r) {
      should.ok(!e);
      r.should.includeEql({'Database' : 'mysql'});

      var num = 3;
      for (var i = 0; i < num; i++) {
        _me.query('SELECT "rolist"', 100, function (e, r) {
          should.ok(!e);
          r.should.eql([{'rolist' : 'rolist'}]);
          if (0 === (--num)) {
            _messages.should.includeEql(['busy', 1, 1, util.format('%s@%s:%d',
                Common.config.user, Common.config.host, Common.config.port)]);
            done();
          }
        });
      }
    });
  });
  /* }}} */

  /* {{{ should_heartbeat_failed() */
  it('should_heartbeat_failed', function (done) {
    var _me = Cluster.create({'maxconnections' : 1});
    _me.on('error', function (e) {
    });

    _me.addserver({'host' : 'localhost', 'port' : 33081});
    _me.setHeartBeatQuery('SELECT 1', function (res) {
      return 0;
    });
    _me.addserver(Common.config);

    var num = 7;
    for (var i = 0; i < 5; i++) {
      _me.query('UPDATE 1', 50, function (e, r) {
        e.should.have.property('name', 'QueueTimeout');
        e.message.should.eql('Query stays in the queue more than 50 ms');
        if (0 === (--num)) {
          done();
        }
      });
    }
    _me.query('UPDATE "full"', 0, function (e, r) {
      e.should.have.property('name', 'QueueIsFull');
      e.message.should.eql('Too many queries queued');
      if (0 === (--num)) {
        done();
      }
    });
    _me.query('select 1', 10, function (e, r) {
      e.should.have.property('name', 'QueueTimeout');
      e.message.should.eql('Query stays in the queue more than 10 ms');
      if (0 === (--num)) {
        done();
      }
    });
  });
  /* }}} */

  /* {{{ should_readonly_works_fine() */
  it('should_readonly_works_fine', function (done) {
    var _me = Cluster.create({'maxconnections' : 1});
    _me.on('error', function (e) {
    });

    var _st = 0;
    _me.setHeartBeatQuery('SELECT 1', function (res) {
      return _st;
    });
    _me.addserver(Common.config);

    var num = 6;
    for (var i = 0; i < 6; i++) {
      _me.query('SELECT 1', 100, function (e, r) {
        if (0 === (--num)) {
          done();
        }
      });
      if (i >= 4) {
        _st = 1;
      }
    }
  });
  /* }}} */

  /* {{{ should_emit_busy_works_fine() */
  it('should_emit_busy_works_fine', function (done) {
    var _me = Cluster.create({'maxconnections' : 1});
    _me.addserver(Common.config);

    var _busyResults = [];
    _me.on('busy', function (queuesize, maxconnections, which) {
      maxconnections.should.eql(1);
      _busyResults.push(queuesize);
    });
    var num = 0;
    for (var i = 0; i < 3; i++) {
      _me.query('SELECT SLEEP(0.1)', function (e, r) {
        num ++;
        should.not.exist(e);
        if (num >= 3) {
          _busyResults.reduce(function (a, b) {
            return a + b;
          }).should.eql(3);
          done();
        }
      });
    }
  });
  /* }}}*/

  it('should_server_no_response_works_fine', function (done) {
    var blocker = getBlocker(33062);
    var _config = Common.extend({
      'host' : 'localhost', 'port' : 33062
    });

    var _me = Cluster.create({'maxconnections' : 1});
    _me.on('error', function (e) {});
    _me.addserver(_config);

    _me.query('SHOW DATABASES', function (e, r) {
      should.not.exist(e);
      Array.isArray(r).should.be.true;

      blocker.block();
      setTimeout(afterBlock, 20);

      function afterBlock() {
        _me.query('SHOW DATABASES', 10, function (e, r) {
          should.exist(e);
          e.name.should.eql('QueryTimeout');
          blocker.open();
          setTimeout(afterOpen, 20);
        });
      }

      function afterOpen() {
        _me.query('SHOW DATABASES', 100, function (e, r) {
          should.not.exist(e);
          done();
        });
      }
    });

  });


});

