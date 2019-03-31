package com.qianfen.scala

import scala.util.Random

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/3/23 12:41
  *
  */
class G_Match {

}

object G_Match {


  def main(args: Array[String]): Unit = {
    //字符串匹配
    var arr = Array("1", "2", "3")

    val ints = arr(Random.nextInt(arr.length))

    ints match {
      case "1" => println("one")
      case "2" => println("two")
      case "3" => println("three")
      case _ => println("垃圾")
    }



    //类型匹配
    var typeArrs = Array(1,2,"3",true)
    val typeArr: Any = typeArrs(Random.nextInt(typeArrs.length))

    typeArr match {
      case str:String =>println(s"$str,$typeArr")
      case double:Double =>println(s"$double,$typeArr")
      case int:Int =>println(s"$int,$typeArr")

    }

  }


}
