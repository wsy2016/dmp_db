package com.dmp.Tag

import cn.dmp.beans.Log

/**
  *
  * Description: APP名称（标签格式：APPxxxx->1）xxxx为APP的名称，
  * 使用缓存文件appname_dict进行名称转换
  *
  * Author: wsy
  *
  * Date: 2019/4/4 15:16
  *
  */
object Tags4App extends Tag {

  override def makeTags(args: Any*): Map[String, Int] = {
    var map = Map[String, Int]()

    if (args.length > 0) {
      val log: Log = args(0).asInstanceOf[Log]
    }
    map
  }
}
