/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var rewire = require('rewire');
var Pool = rewire(__dirname + '/../lib/pool.js');
var Common = require(__dirname + '/common.js');

var Connection = Common.mockConnection();
beforeEach(function () {
  Pool.__set__({
    'Connection' : Connection,
  });
});

describe('mysql pool', function () {

  it('', function () {
    var _me = Pool.create({}, {});
  });

});

