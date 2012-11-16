/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var interceptor = require('interceptor');

var Common = require(__dirname + '/common.js');

var Connection = require(__dirname + '/../lib/connection.js');
var getBlocker = function (port) {
  var cfg = Common.extend();
  var _me = interceptor.create(util.format('%s:%d', cfg.host, cfg.port || 3306));
  _me.listen(port);
  return _me;
};

var getAddress = function (config) {
  return util.format('%s@%s:%d', config.user, config.host, config.port);
};

describe('mysql connection', function () {

  /* {{{ should_got_server_restart_event() */
  it('should_got_server_restart_event', function (done) {

    var blocker = getBlocker(33061);
    var _config = Common.extend({
      'host' : 'localhost', 'port' : 33061
    });

    var _events = [];

    blocker.open();
    var _me = Connection.create(_config);
    _me.on('error', function (e) {
      e.message.should.include(getAddress(_config));
      console.log(e);
    });
    _me.on('close', function (e) {
      console.log('a' + e);
    });

    _me.query('SHOW DATABASES', 100, function (error, res) {
      should.ok(!error);
      res.should.includeEql({'Database' : 'mysql'});

      /**
       * XXX: server 端关闭
       */
      blocker.close();

      done();
      return;
      _me.query('SHOW DATABASES', 100, function (error, res) {
        console.log(error);
      });

      _me.close(function () {
        done();
      });
    });
  });
  /* }}} */

  it('should_connect_timeout_works_fine', function () {
  });

  it('should_query_timeout_works_fine', function () {
  });

});

