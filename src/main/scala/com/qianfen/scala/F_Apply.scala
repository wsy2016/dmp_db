package com.qianfen.scala

/**
  *
  * Description:
  * apply: 初始构造,构造工厂方法(参数-->对象)
  * unapply:(对象-->参数)
  *
  * Author: wsy
  *
  * Date: 2019/3/21 22:58
  *
  */
class F_Apply(val name: String, var age: Int, var faceValue: Int) {

}

object F_Apply {

  def apply(name: String, age: Int): F_Apply = {
    new F_Apply(name, age, 1)
  }

  def unapply(arg: F_Apply): Option[(String, Int, Int)] = {

    Some(arg.name, arg.age, arg.faceValue)

  }

}

object Test {

  def main(args: Array[String]): Unit = {
    val apply: F_Apply = F_Apply("1", 2)

    apply match {
      case F_Apply("1", age, faceValue) => println(s"age :$age")
      case _ =>println("laiji")
    }

  }
}