/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var interceptor = require('interceptor');

var Common = require(__dirname + '/common.js');

var Connection  = require(__dirname + '/../lib/connection.js');

describe('mysql connection', function () {

  /* {{{ should_reconnect_works_fine() */
  it('should_reconnect_works_fine', function (done) {

    var config  = Common.extend();
    var blocker = interceptor.create(util.format('%s:%d', config.host, config.port || 3306));
    blocker.listen(33061);

    blocker.block();

    var _me = Connection.create(Common.extend({
      'host' : 'localhost',
      'port' : 33061
    }));
    _me.query('SHOW DATABASES', 25, function (error, res) {
      error.should.have.property('name', 'QueryTimeout');
      blocker.open();

      setTimeout(function () {
        _me.query('SHOW DATABASES', 20, function (error, res) {
          should.ok(!error);
          _me.close(done);
        });
      }, 100);
    });
  });
  /* }}} */

});

