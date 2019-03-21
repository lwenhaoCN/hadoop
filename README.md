# 该demo包含hbase hive spark
# 测试使用的demo

  cn.lwenhao.hbase.TestHbaseSql 为测试HBase，包含删除表、创建表、插入数据、根据rowKey查询、遍历查询
  
  cn.lwenhao.HiveApplication 启动spring boot程序测试hive与spark，端口号为8085
  
  cn.lwenhao.hive.HiveDataSourceController 和 cn.lwenhao.hive.HiveJdbcTemplateController 测试hive使用
  
  cn.lwenhao.spark.DemoControl 测试spark使用
  
  使用本地文件为：cloudera-scm-server.log 可替换为你的文件，内容可以为任意服务的log文件。
  
  cn.lwenhao.test.TestMysql 中使用到的IPTABLE.sql文件，可以我的git上找到和下载。
