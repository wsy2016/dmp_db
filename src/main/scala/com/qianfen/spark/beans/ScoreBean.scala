package com.qianfen.spark.beans

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/3/28 21:52
  *
  */
class ScoreBean(var id: Int,
                var studentId: Int,
                var language: Int,
                var math: Int,
                var english: Int,
                var className: String,
                var departmentName: String) extends Product with Serializable {
  override def productElement(n: Int): Any = n match {
    case 0 => id
    case 1 => studentId
    case 2 => language
    case 3 => math
    case 4 => english
    case 5 => className
    case 6 => departmentName
    case _ => null

  }

  override def productArity: Int = 7

  //判断that 是否 能够实例化ScoreBean
  override def canEqual(that: Any): Boolean = that.isInstanceOf[ScoreBean]


  override def toString = s"ScoreBean($id, $studentId, $language, $math, $english, $className, $departmentName)"
}

object ScoreBean {

  def apply(array: Array[String]): ScoreBean = new ScoreBean(
    array(0).toInt
    , array(1).toInt
    , array(2).toInt
    , array(3).toInt
    , array(4).toInt
    , array(5)
    , array(6)

  )
}
