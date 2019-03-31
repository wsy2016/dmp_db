package com.dmp.report

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Description:媒体分析(city)报告
  *
  * Author: wsy
  *
  * Date: 2019/2/20 23:04
  *
  */
object AreaAnalyseRpt {


  def main(args: Array[String]): Unit = {

    if (args.length > 3) {
      println(
        """
          |  参数不完全
          |  arg[1] , arg[1] , arg[1]
          |
        """.stripMargin)
      System.out
    }


    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val dataFrame: DataFrame = spark.read.parquet("/Users/wensiyang/Downloads/result/dmpLogResult")
    dataFrame.createOrReplaceTempView("log")
    //val sqlFrame: DataFrame = spark.sql("select * from log")

    val sqlFrame: DataFrame = spark.sql("select provincename,cityname ,count(1) from log group by `provincename`,`cityname` ")
    sqlFrame.show(100)


  }

}
