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
    'Variable_Name' : 'READ_ONLY', 'Value' : 'OFF'
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

    _me._queue.should.eql([]);
    _me._stack.should.eql([]);

    ['state', 'error'].forEach(function (i) {
      _me.on(i, function () {
      });
    });

    var num = 9;
    for (var i = 0; i < num; i++) {
      _me.query('SELECT ' + i, function (e, r) {
        if (0 !== (--num)) {
          return;
        }
        process.nextTick(function () {
          _me._queue.should.eql([]);
          // 1,2,3,4,1,[2,3,4,1]
          _me._stack.should.eql([2,3,4,1]);
          _me.query('should use 1', function (e,r) {
            _me._queue.should.eql([]);
            _me._stack.should.eql([2,3,4]);
            process.nextTick(function () {
              _me._stack.should.eql([2,3,4,1]);
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

    Connection.__emitEvent(0, 'error', 'aa');
    Connection.__emitEvent(0, 'close');
    setTimeout(function () {
      _messages.should.eql([
        ['state'],          /**<  error引起 */
        ['error', 'aa'],
        /**<  一次正常，一次error后恢复 */
        ['state', [{'Variable_Name' : 'READ_ONLY', 'Value' : 'OFF'}]],
        ['state', [{'Variable_Name' : 'READ_ONLY', 'Value' : 'OFF'}]],
        ]);

      _messages = [];
      _me.setHeartBeatQuery('HEARTBEAT error');
      setTimeout(function () {
        _messages.should.eql([
          ['state'],
          ['error', 'TestError'],
          ['error', 'TestError'],
          ]);
        done();
      }, 60);
    }, 60);
  });
  /* }}} */

  /* {{{ should_close_query_connection_when_error() */
  it('should_close_query_connection_when_error', function (done) {
    var _me = Pool.create({'maxconnections' : 2});
    _me.query('select fatal', function (e, r) {
      e.should.eql({'fatal' : true, 'name' : 'TestFatal'});
      done();
    });
  });
  /* }}} */

  /* {{{ should_idletime_works_fine() */
  it('should_idletime_works_fine', function (done) {
    var _me = Pool.create({'maxconnections' : 2, 'maxidletime' : 10});
    _me.query('query1', function (e, r) {
      _me._stack.should.eql([]);
      process.nextTick(function () {
        _me._stack.should.eql([1]);
        setTimeout(function () {
          _me.query('query2', function (e, r) {
            process.nextTick(function () {
              _me._stack.should.eql([1]);
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

});

