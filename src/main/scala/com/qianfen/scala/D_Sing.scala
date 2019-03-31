package com.qianfen.scala

import scala.collection.mutable.ArrayBuffer

/**
  *
  * Description: scala没有静态方法和静态字段,都是object类实现
  *
  * 工具类(常量/工具方法)
  * 实现单例对象
  *
  * Author: wsy
  *
  * Date: 2019/3/21 22:25
  *
  */
object D_Sing {

  def main(args: Array[String]): Unit = {
    val factory: SessionFactory.type = SessionFactory

    println(factory.getSession(0))
    println(factory.getSession(2))
    println(factory.getSession(1))
  }
}

object SessionFactory {

  /////////////////////////
  println("SessionFactory被执行")

  var i = 5

  private val sessions = new ArrayBuffer[Session]

  while (i > 0) {
    sessions += new Session
    i -= 1

  }
      //相当于JAVA静态代码块
  /////////////////////////

  val getSession : ArrayBuffer[Session] = {
      sessions
  }


}

class Session {}