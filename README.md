[![Build Status](https://secure.travis-ci.org/aleafs/easymysql.png?branch=develop)](http://travis-ci.org/aleafs/easymysql)

Click **[here](http://aleafs.github.com/coverage/easymysql.html)** to get the details of test coverage.

正在开发中，尚不完备。

## About

`easymysql` 基于[`node-mysql`](https://github.com/felixge/node-mysql) 开发而来，提供一个简单、高可用的mysql连接基础库。主要特性如下：

* 支持query超时控制；
* 可控制的连接池支持，SQL总是尽可能早地被**可用的**空闲连接抢到并执行；
* 支持master-slave模式，基于`SHOW VARIABLES LIKE 'READ_ONLY'`方式自动判断主库和从库，运行期间自动感知主从切换；
* 事务支持。

## Install

```bash
$ npm install easymysql
```

## Usage

```javascript

var Client = require('easymysql');

var mysql = Client.create({
  'maxconnection' : 10
});

mysql.addserver({
  'host' : '127.0.0.1',
  'user' : 'write_user',
  'password' : ''
});
mysql.addserver({
  'host' : '127.0.0.1',
  'user' : 'read_user',
  'password' : ''
});

mysql.query('SHOW DATABASES', function (error, res) {
  console.log(res);
});

```

## License

(The MIT License)

Copyright (c) 2012 aleafs and other easymysql contributors

Permission is hereby granted, free of charge, to any person obtaining
a copy of this software and associated documentation files (the
'Software'), to deal in the Software without restriction, including
without limitation the rights to use, copy, modify, merge, publish,
distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so, subject to
the following conditions:

The above copyright notice and this permission notice shall be
included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED 'AS IS', WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
