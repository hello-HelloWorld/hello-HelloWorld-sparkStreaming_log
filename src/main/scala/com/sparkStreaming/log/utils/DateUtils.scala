package com.sparkStreaming.log.utils

import org.apache.commons.lang3.time.FastDateFormat


/*
* @author: sunxiaoxiong
* @date  : Created in 2020/7/10 16:21
*/

/*
* 格式化日期工具类
* */
object DateUtils {
  //指定输入的日期格式
  val YYYYMMDDHMMSS_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd hh:mm:ss")
  //指定输出的日期格式
  val TARGET_FORMAT: FastDateFormat = FastDateFormat.getInstance("yyyyMMddmmhhss")

  def getTime(time: String) = {
    YYYYMMDDHMMSS_FORMAT.parse(time).getTime
  }

  def parseToMinute(time: String) = {
    TARGET_FORMAT.format(getTime(time))
  }
}
