package com.qianfen.scala

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/3/20 23:04
  *
  */
object B_Array {

  def main(args: Array[String]): Unit = {

    //定长数组
    val strings: Array[String] = new Array[String](10)
    val array: Array[Any] = Array(1, 2, 3, 45, "s")
    strings(0) = "0";
    strings(1) = "1";
    strings(2) = "1";
    strings(3) = "1";
    strings(4) = "1";
    strings(5) = "1";
    //strings.foreach(println(_))
    //array.foreach(println(_))

    val buffer: ArrayBuffer[Int] = ArrayBuffer(1, 2, 34, 5)
    buffer.foreach(println(_))
    //不定长数组才能+=
    buffer += 10

    val arrayBuffer: ArrayBuffer[String] = new ArrayBuffer[String](1)
    arrayBuffer+="2"
    arrayBuffer+="2"
    arrayBuffer+="2"
    arrayBuffer.foreach(println(_))
  }


}
