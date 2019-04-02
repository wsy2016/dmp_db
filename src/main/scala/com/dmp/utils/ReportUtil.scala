package com.dmp.utils

import cn.dmp.beans.Log

/**
  *
  * Description:报表辅助工具类
  *
  * Author: wsy
  *
  * Date: 2019/2/15 10:09
  *
  */
object ReportUtil {

  /**
    *
    * Description: 统计请求数
    * Author: wsy
    * Date: 2019/2/15 10:19
    * Param: [log]
    * Return: 总请求,有效请求,广告请求
    */
  def calculateRequest(log: Log): List[Double] = {
    if (log.requestmode == 1) {
      //Requestmode:Int 数据请求方式（1：请求，2：展示，3：点击）
      //Processnode:Int 流程节点（1：请求量ktp 2 :有效请求 3：广告请求）
      if (log.processnode == 1) {
        List(1, 0, 0)
      } else if (log.processnode == 2) {
        List(1, 1, 0)
      } else if (log.processnode == 3) {
        List(1, 1, 1)
      } else {
        List(0, 0, 0)
      }
    } else {
      List(0, 0, 0)

    }

  }

  def calculateResponse(log: Log): List[Double] = {
    if (log.adplatformproviderid >= 100000 && log.iseffective == 1 && log.isbilling == 1) {
      if (log.isbid == 1 && log.adorderid != 0) {
        List(1, 0)
      } else if (log.iswin == 1) {
        List(0, 1)
      } else {
        List(0, 0)
      }
    } else {
      List(0, 0)
    }
  }


  /**
    * 计算展示量和点击量
    *
    * @param log 输入的日志对象
    * @return 展示量  点击量
    */
  def calculateShowClick(log: Log): List[Double] = {
    if (log.iseffective == 1) {
      if (log.requestmode == 2) {
        List(1, 0)
      } else if (log.requestmode == 3) {
        List(0, 1)
      } else {
        List(0, 0)
      }
    } else {
      List(0, 0)
    }

  }

  /**
    * 用于计算广告消费和广告成本
    *
    * @param log
    * @return
    */
  def calculateAdCost(log: Log): List[Double] = {
    if (log.adplatformproviderid >= 100000
      && log.iseffective == 1
      && log.isbilling == 1
      && log.iswin == 1
      && log.adorderid >= 200000
      && log.adcreativeid >= 200000) {
      List(log.winprice / 1000, log.adpayment / 1000)
    } else {
      List(0.0, 0.0)
    }

  }

}
