package com.dmp.report

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  *
  * Description:媒体分析(App)报告 checkPoint,广播变量
  *
  * Author: wsy
  *
  * Date: 2019/4/3 10:12
  *
  */
object AppReportV2 {


  private final val RESUREC_PATH: String = "/Users/wensiyang/Downloads/dmp/resources/dmpLog.txt"
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val appMap: Map[String, String] = sc.textFile(RESUREC_PATH).flatMap(line => {
    }


    }
}
