package com.dmp.utils

/**
  *
  * Description: 数据格式转化
  *
  * Author: wsy
  *
  * Date: 2019/1/30 13:52
  *
  */
object NBUtil {


  def toInt(str: String): Int = {
    try {
      str.toInt
    } catch {
      case _: Exception => 0
    }

  }

  def toDouble(str:String):Double={
    try {
      str.toDouble
    }catch {
      case ex:Exception =>{
        println(ex)
        0.0

      }
    }
  }

  def main(args: Array[String]): Unit = {
    println(this.toDouble("dd12xzcdsc"))
  }

}
