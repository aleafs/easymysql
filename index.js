/* vim: set expandtab tabstop=2 shiftwidth=2 foldmethod=marker: */

"use strict";

var LIBPATH = __dirname + (process.env.MYSQL_CLUSTER_COV ? '/lib-cov' : '/lib');

module.exports = require(LIBPATH + '/cluster.js');

