/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var rewire = require('rewire');
var Pool = rewire(__dirname + '/../lib/pool.js');
var Common = require(__dirname + '/common.js');

var Connection = Common.mockConnection();

beforeEach(function () {
  Connection.makesureCleanAllData();
  Connection.__mockQueryResult(/SHOW\s+Variables\s+like\s+"READ_ONLY"/i, [{
    'Variable_name' : 'READ_ONLY', 'Value' : 'OFF'
  }]);
  Connection.__mockQueryResult(/error/i, undefined, 'TestError');
  Connection.__mockQueryResult(/fatal/i, undefined, {'fatal' : true, 'name' : 'TestFatal'});

  Pool.__set__({
    'HEARTBEAT_TIMEOUT' : 5,
    'Connection' : Connection,
  });
});

describe('mysql pool', function () {

  /* {{{ should_mysql_pool_works_fine() */
  it('should_mysql_pool_works_fine', function (done) {
    var _me = Pool.create({
      'maxconnections' : 4,
    });

    _me._queue.size().should.eql(0);
    _me._stack.should.eql([]);

    ['state', 'error'].forEach(function (i) {
      _me.on(i, function () {
      });
    });
    var num = 9;
    for (var i = 0; i < num; i++) {
      _me.query('SELECT ' + i, 0, function (e, r) {
        if (0 !== (--num)) {
          return;
        }
        process.nextTick(function () {
          _me._queue.size().should.eql(0);
          // 1,2,3,4,1,[2,3,4,1]
          var expect = [];
          _me._stack.forEach(function (c) {
            expect.push(_me._conns.indexOf(c));
          });
          expect.should.eql([2,3,4,1]);

          _me.query('should use 1', 0, function (e,r) {
            _me._queue.size().should.eql(0);
            var expect = [];
            _me._stack.forEach(function (c) {
              expect.push(_me._conns.indexOf(c));
            });
            expect.should.eql([2,3,4]);
            process.nextTick(function () {
              var expect = [];
              _me._stack.forEach(function (c) {
                expect.push(_me._conns.indexOf(c));
              });
              expect.should.eql([2,3,4,1]);
              done();
            });
          });
        });
      });
    }
  });
  /* }}} */

  /* {{{ should_heartbeat_works_fine() */
  it('should_heartbeat_works_fine', function (done) {
    var _me = Pool.create({'maxconnections' : 2});

    var _messages = [];
    ['state', 'error'].forEach(function (i) {
      _me.on(i, function (e) {
        _messages.push([i].concat(Array.prototype.slice.call(arguments)));
      });
    });

    setTimeout(function () {
      _messages.should.eql([
        ['state', [{'Variable_name' : 'READ_ONLY', 'Value' : 'OFF'}]]]);

      _messages = [];
      _me.setHeartBeatQuery('HEARTBEAT error');
      setTimeout(function () {
        _messages.should.eql([
          ['state'],
          ['error', 'TestError'],
          ]);
        done();
      }, 60);
    }, 120);
  });
  /* }}} */

  /* {{{ should_close_query_connection_when_error() */
  it('should_close_query_connection_when_error', function (done) {
    var _me = Pool.create({'maxconnections' : 2});
    _me.query('select fatal', 0, function (e, r) {
      e.should.eql({'fatal' : true, 'name' : 'TestFatal'});
      done();
    });
  });
  /* }}} */

  /* {{{ should_idletime_works_fine() */
  it('should_idletime_works_fine', function (done) {
    var _me = Pool.create({'maxconnections' : 2, 'maxidletime' : 10});
    _me.query('query1', 0, function (e, r) {
      _me._stack.should.eql([]);
      process.nextTick(function () {
        _me._stack.length.should.eql(1);
        setTimeout(function () {
          _me.query('query2', 0, function (e, r) {
            process.nextTick(function () {
              _me._stack.length.should.eql(1);
              setTimeout(function () {
                _me._stack.should.eql([]);
                done();
              }, 11);
            });
          });
        }, 8);
      });
    });
  });
  /* }}} */

  it('should_when_idletimeout_removed_then_stack_and_conns_match', function (done) {
    var _me = Pool.create({'maxconnections' : 3, 'maxidletime' : 10});
    function makeConcurrent(cb) {
      for (var i = 0; i < 3; i ++) {
        _me.query('query1', 0, function (e, r) {
          _ok();
        });
      }
      var count = 0;
      function _ok() {
        count ++;
        if (3 === count) {
          process.nextTick(function () {
           cb();
          });
        }
      }
    }
    makeConcurrent(function () {
      _me._stack.length.should.eql(3);
      _me._conns.length.should.eql(4);
      setTimeout(function () {
        _me._stack.should.eql([]);
        done();
      }, 12);
    });


  });

  /* {{{ should_queue_timeout_works_fine() */
  it('should_queue_timeout_works_fine', function (done) {
    var _me = Pool.create({'maxconnections' : 1});

    var num = 2;
    _me.query('SLEEP 11', 15, function (e, r) {
      if (0 === (--num)) {
        done();
      }
    });

    /**
     * XXX: 只有一条连接
     */
    _me.query('QUEUED', 10, function (e, r) {
      e.should.have.property('name', 'QueueTimeout');
      e.message.should.include('Query stays in the queue more than 10 ms (test)');
      if (0 === (--num)) {
        done();
      }
    });
  });
  /* }}} */

  it('should_get_fatal_error_works_fine', function (done) {
    var _me = Pool.create({'maxconnections' : 1});
    _me.query('SELECT fatal', 20, function (e, r) {
      should.exist(e);
      should.ok(e.fatal);
    });
    _me.query('SHOW Variables like "READ_ONLY"', 0, function (e, r) {
      should.not.exist(e);
      done();
    });
  });

  it('should_connecton_random_emit_error_works_fine', function (done) {
    var _me = Pool.create({'maxconnections' : 1});
    _me.on('error', function (e) {
      should.exist(e);
      e.message.should.eql('myError');
    });

    _me.query('SHOW Variables like "READ_ONLY"', 20, function (e, r) {
      should.not.exist(e);
    });

    var e = new Error('myError');
    e.fatal = 1;
    Connection.__emitEvent(1, 'error', e);

    _me.query('SHOW Variables like "READ_ONLY"', 0, function (e, r) {
      should.not.exist(e);
      done();
    });
  });

  it('when_all_connection_flag_is_unuseable_pool_works_fine', function (done) {
    var _me = Pool.create({'maxconnections' : 2});
    var _r = [];
    for (var i = 0; i < 5; i ++) {
      _me.query('SHOW Variables like "READ_ONLY"', 10, function (e, r) {
        _r.push(e || r);
      });
    }
    var conns = Connection.__connectionNum();

    setTimeout(function () {
      [1, 2].forEach(function (i) {
        Connection.__setFlag(i, -1);
      });
    }, 5);

    setTimeout(function () {
      _me.query('SHOW Variables like "READ_ONLY"', 10, function (e, r) {
        should.not.exist(e);
        done();
      });
    }, 10);
  });

});

