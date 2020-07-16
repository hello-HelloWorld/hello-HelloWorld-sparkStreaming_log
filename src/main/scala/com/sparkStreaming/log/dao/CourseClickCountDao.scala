package com.sparkStreaming.log.dao

import com.sparkStreaming.log.domain.CourseClickCount
import com.sparkStreaming.log.utils.HbaseUtils
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/7/10 16:51
*/
/*
* 实战课程点击数统计访问层
* 用于交互HBase，把课程点击数的统计结果写入HBase
* */
object CourseClickCountDao {
  //表名
  val tableName = "ns1:courses_clickcount"
  //列族
  val cf = "info"
  //列
  val qualifer = "click_count"

  //保存数据到hbase表中
  //list: ListBuffer[CourseClickCount] 当天没门课程的总点击数
  def save(list: ListBuffer[CourseClickCount]): Unit = {
    val table: HTable = HbaseUtils.getInstance().getHtable(tableName)
    for (item <- list) {
      //调用hbase的一个自增方法
      table.incrementColumnValue(Bytes.toBytes(item.day_course), Bytes.toBytes(cf), Bytes.toBytes(qualifer), item.click_count)
    }
  }
}
