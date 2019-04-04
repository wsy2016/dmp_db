package com.dmp.Tag

import cn.dmp.beans.Log

/**
  *
  * Description:关键词（标签格式：Kxxx->1）xxx为关键字。关键词个数不能少于3个字符，且不能超过8个字符；关键字中如包含”|”,则分割成数组，转化成多个关键字标签
  *
  *
  * Author: wsy
  *
  * Date: 2019/4/4 15:50
  *
  */
object Tags4keyWord extends Tag {


  override def makeTags(args: Any*): Map[String, Int] = {
    var map: Map[String, Int] = Map[String, Int]()
    if (args.length > 0) {
      val log: Log = args(0).asInstanceOf[Log]
      val keyword = log.keywords.split("\\|")
      keyword.foreach(kw =>{
        map += ("K"+kw->1)
      })
    }
    map
  }
}
