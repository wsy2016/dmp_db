package com.dmp.report

import java.util.Properties

import com.dmp.utils.{ConfUtil}
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  *
  * Description: 媒体分析(App)报告
  *
  * Author: wsy
  *
  * Date: 2019/2/15 10:49
  *
  */
object AppReport {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(
        """
          |com.dmp.report.AppReport <logDataPath> <appMappingPath> <outputPath>
          | <logDataPath> 日志目录
          | <appMappingPath> 映射文件目录
          | <outputPath> 输出结果文件目录
        """.stripMargin)
      System.exit(0)
    }

    val Array(logDataPath, appMappingPath, outputPath) = args
    val spark: SparkSession = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //为了方便修改清洗后的数据,落地到mysql
    //val df: DataFrame = spark.read.parquet(logDataPath)
    val url = ConfUtil.getString("jdbc.url")
    val tableName = ConfUtil.getString("jdbc.tableName")
    val user = ConfUtil.getString("jdbc.user")
    val password = ConfUtil.getString("jdbc.password")

    val connProp = new Properties()
    connProp.put("user", user)
    connProp.put("password", password)
    val df: DataFrame = spark.read.jdbc(url, tableName, connProp)
    df.rdd.map(x => {
      line2Log2(x)
    })
    spark.stop()
  }

  def line2Log2(row: Row) = {
    val str: String = row.toString()
    println(str)
  }
}
