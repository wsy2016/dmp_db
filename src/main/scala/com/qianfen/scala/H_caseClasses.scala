package com.qianfen.scala

import scala.util.Random

/**
  *
  * Description: 样例类
  *
  * Author: wsy
  *
  * Date: 2019/3/23 13:34
  *
  */
object H_caseClasses {

  def main(args: Array[String]): Unit = {
    val arr = Array(CheckTimeOutTask,HearBeat(100),SubmitTask("1","task"))

     arr(Random.nextInt(arr.length)) match {

      case CheckTimeOutTask =>println("CheckTimeOutTask")
      case HearBeat(time) =>println("HearBeat")
      case SubmitTask(id,task) =>println("SubmitTask")

    }
  }

}


case class HearBeat(time: Long)

case class SubmitTask(id: String, task: String)

case object CheckTimeOutTask