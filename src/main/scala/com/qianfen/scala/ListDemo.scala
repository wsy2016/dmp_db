package com.qianfen.scala

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/3/31 16:23
  *
  */
object ListDemo {

  def main(args: Array[String]): Unit = {


    val ints: List[Int] = List(1,24,6,7)
//    ints.sortBy(x=>x).reverse.foreach(println(_))
//    ints.sortWith(_<_).foreach(println(_))

    val list = List(("我平时","a","A",1,2))
    list.sortBy(_._4)(Ordering.Int.reverse).foreach(println(_))

  }
}
