## 设计思路

* lib/connection.js : 维护一个连接，不自己做重连，主动close或者被动close(error引起)都emit一个close事件；
* lib/pool.js : 维护单机mysql的连接池；一个连接用来做心跳，任何原因引起的close都会尝试重连；心跳之外采用动态大小的连接池处理query；
* lib/cluster.js : 主从切换判断，机器之间的流量分配策略；

