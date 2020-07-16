package com.sparkStreaming.log.dao

import com.sparkStreaming.log.domain.CourseSearchClickCount
import com.sparkStreaming.log.utils.HbaseUtils
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.util.Bytes

import scala.collection.mutable.ListBuffer

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/7/10 17:31
*/
/*
* 用于交互HBase，把搜引擎搜索数量的统计结果写入HBase
* */
object CourseSearchClickCountDao {

  val tableName = "ns1:courses_search_clickcount"
  val cf = "info"
  val qualifer = "click_count"

  def save(list: ListBuffer[CourseSearchClickCount]): Unit = {
    val table: HTable = HbaseUtils.getInstance().getHtable(tableName)
    for (item <- list) {
      table.incrementColumnValue(Bytes.toBytes(item.day_serach_course), Bytes.toBytes(cf), Bytes.toBytes(qualifer), item.click_count)
    }
  }
}
