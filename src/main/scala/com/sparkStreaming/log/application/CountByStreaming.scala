package com.sparkStreaming.log.application

import com.sparkStreaming.log.dao.{CourseClickCountDao, CourseSearchClickCountDao}
import com.sparkStreaming.log.domain.{ClickLog, CourseClickCount, CourseSearchClickCount}
import com.sparkStreaming.log.utils.DateUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jackson.map.deser.std.StringDeserializer

import scala.collection.mutable.ListBuffer


/*
* @author: sunxiaoxiong
* @date  : Created in 2020/7/10 17:49
*/

/*
* 用于处理数据，是本项目的程序入口，最为核心的类
*
* 日志格式：
* 134.63.132.156	2020-07-13 14:16:01	"GET /class/112.html HTTP/1.1"	200	https://www.sogou.com/web?query=Spark SQL实战
* 10.72.29.110	2020-07-13 14:16:01	"GET /learn/500 HTTP/1.1"	200	http://www.baidu.com/s?wd=SpringBoot实战
* 134.55.168.111	2020-07-13 14:16:01	"GET /class/131.html HTTP/1.1"	404	http://cn.bing.com/search?q=Vue.js
* */
object CountByStreaming {
  def main(args: Array[String]): Unit = {
    /**
      * 最终该程序将打包在集群上运行，
      * 需要接收几个参数：zookeeper服务器的ip，kafka消费组，
      * 主题，以及线程数
      */
    if (args.length != 4) {
      System.err.println("传入的参数不正确")
      System.exit(1)
    }
    //接收main方法的参数（外面出入的参数）
    val Array(zkAddress, group, topics, threadNum) = args

    /**
      * 创建Spark上下文，本地运行需要设置AppName
      * Master等属性，打包上集群前需要删除
      */
    val conf: SparkConf = new SparkConf().setAppName("count").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    //创建sparkstreaming,每隔60秒接收数据
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(60))

    //使用kafka作为数据源
    val topicSet: Set[String] = topics.split(",").toSet
    //kafka的地址
    val brobrokers = "192.168.56.150:9092,192.168.56.151:9092,192.168.56.152:9092"
    //kafka消费者参数配置
    val kafkaParam = Map(
      "bootstrap.servers" -> brobrokers, //用于初始化连接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> group,
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上,可以使用这个配置属性
      //可以使用这个配置,latest自动重置偏移量为最新的偏移量
      "auto.offest.reset" -> "latest",
      //如果是true,则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
      //      ConsumerConfig.GROUP_ID_CONFIG
    );
    //创建kafka离散流，每隔60秒消费一次kafka集群的数据
    val kafkaInputDS: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](ssc, LocationStrategies.PreferConsistent, ConsumerStrategies.Subscribe[String, String](topicSet, kafkaParam))

    //得到原始的日志数据,流中的每一条数据就是kafka的每一条数据
    val logResourcesDS: DStream[String] = kafkaInputDS.map(_.value())
    /*
    * 1.清洗数据，把它封装到ClickLog中
    * 2，过滤掉非法的数据
    * */
    val cleanDataRDD: DStream[ClickLog] = logResourcesDS.map(line => {
      val splits: Array[String] = line.split("\t")
      if (splits.length != 5) {
        //不合法的数据直接封装默认赋予错误值，filter会将其过滤
        ClickLog("", "", 0, 0, "")
      } else {
        //访问日志的ip
        val ip: String = splits(0)
        //获得访问日志的时间，并将它格式化
        val time: String = DateUtils.parseToMinute(splits(1))
        //获得搜索的url
        val url: String = splits(2).split(" ")(1)
        //日志访问的状态码
        val statusCode = splits(3).toInt
        //访问的referer信息
        val referer: String = splits(4)
        var courseId = 0
        if (url.startsWith("/class")) {
          val courseIdHtml: String = url.split("/")(2)
          courseId = courseIdHtml.substring(0, courseIdHtml.lastIndexOf(".")).toInt
        }
        //将清洗后的数据封装到ClickLog中
        ClickLog(ip, time, courseId, statusCode, referer)
      }
      //过滤掉非法的数据
    }).filter(clicklog => clicklog.courseId != 0)

    /**
      * (1)统计数据
      * (2)把计算结果写进HBase
      */
    cleanDataRDD.map(line => {
      //这里相当于定义HBase表"ns1:courses_clickcount"的RowKey，
      // 将‘日期_课程’作为RowKey,意义为某天某门课的访问数
      //映射为元组
      (line.time.substring(0, 8) + "_" + line.courseId, 1)
    }).reduceByKey(_ + _) //聚合
      //一个DStream中有多个RDD
      .foreachRDD(rdd => {
      rdd.foreachPartition(partition => {
        val list: ListBuffer[CourseClickCount] = new ListBuffer[CourseClickCount]
        //一个partition中有多条记录
        partition.foreach(item => {
          list.append(CourseClickCount(item._1, item._2))
        })
        //保存到Hbase
        CourseClickCountDao.save(list)
      })
    })

    /**
      * 统计至今为止通过各个搜索引擎而来的实战课程的总点击数
      * (1)统计数据
      * (2)把统计结果写进HBase中去
      */
    cleanDataRDD.mapPartitions(partition => {
      partition.map(line => {
        val referer: String = line.referer
        val time: String = line.time.substring(0, 8)
        var url = ""
        //过滤非法url
        if (referer == "-") {
          (url, time)
        } else {
          //取出搜索引擎的名字
          url = referer.replaceAll("//", "/").split("/")(1)
          (url, time)
        }
      })
    }).filter(x => x._1 != "").map(line => {
      //这里相当于定义HBase表"ns1:courses_search_clickcount"的RowKey，
      // 将'日期_搜索引擎名'作为RowKey,意义为某天通过某搜索引擎访问课程的次数
      //映射为元组
      (line._2 + "_" + line._1, 1)
    }).reduceByKey(_ + _)
      .foreachRDD(rdd => {
        rdd.foreachPartition(partition => {
          val list: ListBuffer[CourseSearchClickCount] = new ListBuffer[CourseSearchClickCount]
          partition.foreach(line => {
            list.append(CourseSearchClickCount(line._1, line._2))
          })
          CourseSearchClickCountDao.save(list)
        })
      })
    ssc.start()
    ssc.awaitTermination()
  }
}
