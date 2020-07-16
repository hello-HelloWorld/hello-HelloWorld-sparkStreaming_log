# sparkStreaming_log
大数据项目案例-实时流处理日志
# 1.技术架构：
python，Flume，kafka，sparkStreaming，hbase，mysql，ssm框架，echarts

# 2.项目流程：
每一分钟调用python脚本产生日志信息存放在服务器目录文件中。

flume监控文件，将收集的日志传给kafka

sparkStreaming消费kafka中的数据，将计算后的结果保存在hbase中

前端ssm整合读取HBase的数据，并且响应jsp页面的Ajax请求，将数据以JSON格式发给前端页面，最后前端页面使用Echarts展示数据。
