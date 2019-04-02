package com.qianfen.scala

/**
  *
  * Description: 与类名相同且用object修饰的对象
  *
  *     类与伴生对象相互访问
  *
  * Author: wsy
  *
  * Date: 2019/3/21 22:47
  *
  */
class E_Dog {

   var name = "狗"

  def printName :Unit = {
    println(E_Dog.SONG)
  }
}

object E_Dog{

  private  val SONG = "呵呵"
  def main(args: Array[String]): Unit = {

    val dog: E_Dog = new E_Dog
    dog.name
    dog.printName
  }


}