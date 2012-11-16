/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should = require('should');
var rewire = require('rewire');
var Pool = rewire(__dirname + '/../lib/pool.js');

beforeEach(function () {
  Pool.__set__({
    'Connection' : '',
  });
});

describe('mysql pool', function () {
});

