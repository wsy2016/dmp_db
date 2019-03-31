package com.qianfen.scala

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/3/20 22:55
  *
  */
object A_For {

  def main(args: Array[String]): Unit = {

    for(i<- 1 to 10){
      println(i)
    }

    for(i<- 1.to(10)){
      println(i)
    }

    val array: Array[Any] = Array(1,2,"1.00")

    for(i<- array){
      println(i)
    }

    for(i<- 1 to 10){
      for(j<- 2 to 10){
        if(i!=j){
          println(10*i)
        }
      }
    }


    for(i<- 1 to 3;j<- 1 to 3;if(i!=j)){
      println(i*10 + j)

    }
  }

}
