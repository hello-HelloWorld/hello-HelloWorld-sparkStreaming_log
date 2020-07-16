package com.sparkStreaming.log.domain

/*
* @author: sunxiaoxiong
* @date  : Created in 2020/7/10 16:47
*/
/**
  * 封装统计通过搜索引擎多来的实战课程的点击量
  *
  * @param day_serach_course 当天通过某搜索引擎过来的实战课程
  * @param click_count       点击数
  */
case class CourseSearchClickCount(day_serach_course: String, click_count: Int)
