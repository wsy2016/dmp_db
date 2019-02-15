package com.dmp.utils

import java.text.SimpleDateFormat

import org.apache.ivy.util.StringUtils

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/1/28 15:16
  *
  */
object TimeUtil {


  def caculateTimeDiff(beginTime: String, endTime: String, pattern: String): Long = {

    if (null == pattern || pattern.length() == 0) {
      val pattern = "yyyyMMddHHmmss"
    }

    val simple = new SimpleDateFormat(pattern)
    val bigin = simple.parse(beginTime).getTime
    val end = simple.parse(endTime).getTime
    end - bigin
  }

}
