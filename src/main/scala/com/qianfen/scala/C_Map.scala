package com.qianfen.scala

/**
  *
  * Description: 映射
  *
  * Author: wsy
  *
  * Date: 2019/3/20 23:20
  *
  */
class C_Map {

  def main(args: Array[String]): Unit = {
    val intToInt: Map[Int, Int] = Map(1 -> 1, 2 -> 123)
   val map: Map[Any, Int] = Map((2,2),(3,4),("2",3),(3,5))
    map.foreach(println(_))
  }


}
