package com.dmp.Tag

import com.google.common.base.Joiner

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/4/3 10:38
  *
  */
trait Tag {

  val joiner: Joiner = Joiner.on("")

  def makeTags(args: Any*): Map[String, Int]
}
