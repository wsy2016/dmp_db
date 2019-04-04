package com.dmp.Tag

import cn.dmp.beans.Log



/**
  *
  * Description:广告位类型（标签格式：LC03->1或者LC16->1）xx为数字，小于10 补0
  *
  * Author: wsy
  *
  * Date: 2019/4/3 10:40
  *
  */
object Tags4Local extends Tag {


  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()
    if (args.length > 0) {
      val log: Log = args(0).asInstanceOf[Log]

      //adspacetype广告位类型（1：banner 2：插屏 3：全屏）
      log.adspacetype match {
        case x if x < 10 => map += ("LC0" + x -> 1)
        case x if x > 10 => map += ("LC" + x -> 1)
      }
    }
    map
  }
}
