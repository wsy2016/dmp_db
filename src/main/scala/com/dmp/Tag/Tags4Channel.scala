package com.dmp.Tag

import cn.dmp.beans.Log
import org.apache.commons.lang3.StringUtils

/**
  *
  * Description:标签格式：CNxxxx->1）xxxx为渠道ID
  *
  * Author: wsy
  *
  * Date: 2019/4/4 15:31
  *
  */
object Tags4Channel extends Tag {
  override def makeTags(args: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    if (args.length > 0) {
      val log: Log = args(0).asInstanceOf[Log]
      val channelid: String = log.channelid
      if (StringUtils.isNotBlank(channelid)) {
        val str: String = joiner.join("CN",channelid)
        map += (str -> 1)
      }
    }
    map


  }
}
