/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

var util = require('util');
var should  = require('should');
var rewire  = require('rewire');
var Common  = require(__dirname + '/common.js');
var Cluster = rewire(__dirname + '/../lib/main.js');

describe('mysql cluster', function () {

  it('should_mysql_cluster_works_fine', function (done) {
    var _me = Cluster.create({'maxconnections' : 2});
    //_me.addserver();
    done();
  });

});

