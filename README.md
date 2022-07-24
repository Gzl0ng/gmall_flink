# gmall_flink
电商实时数仓

1.在原有离线数仓数据结构基础上减少了DWT层，增加了DWM层辅助DWS层，引入clickhouse引擎用作ADS层查询DWS层宽表  
2.采用flinkCDC同步全库业务数据，数据管道采用kafka  
3.日志数据采集用springBoot整合kafka的API接口部署在二台机器，通过主节点nginx代理方式轮询访问这二个数据接口  
4.通过配置表在项目不重启情况下业务数据库表的数量增加也能导入对应Topic，维度表通过经phoenix封装的hbase数据库来保证吞吐量和实时性  
5.维度数据采用redis作缓存，主要计算逻辑使用FlinkAPI开发，部分采用FlinkSQL（注：Flink版本为1.13，官网显示已经支持标准SQL）  
6.数据可视化通过百度的sugar服务，主节点提供API接口，通过花生壳软件允许外网访问主节点接口  
