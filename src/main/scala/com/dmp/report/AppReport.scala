package com.dmp.report

import cn.dmp.beans.Log
import com.dmp.utils.JdbcConnUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Description:媒体分析(App)报告
  *
  * Author: wsy
  *
  * Date: 2019/4/2 11:41
  *
  */
object AppReport {

  private final val inputFile = "/Users/wensiyang/Downloads/dmp/result/dmpLogResult";

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()
    val (url, table, connProp) = JdbcConnUtil.getConn("jdbc.tableName")
    val dataFrame: DataFrame = spark.read.jdbc(url, table, connProp)
    dataFrame.createOrReplaceTempView("logs")

    // select (case provincename when '安徽' then 'Ah' else 'Nan' end) '简称' from logs

    val frame: DataFrame = spark.sql(
      """
        | SELECT
        |     SUM(CASE WHEN requestmode = 1 THEN 1 ELSE 0 END) `总请求数`,
        |     SUM(CASE WHEN requestmode = 1 AND processnode = 2 THEN 1 else 0 end) `有效请求数`,
        |     SUM(CASE WHEN requestmode = 1 AND processnode = 3 THEN 1 else 0 end) `广告请求数`,
        |     appName
        | FROM logs
        | GROUP BY appName
      """.stripMargin)
    frame.createOrReplaceTempView("temp")

    //frame.show()

    val frame1: DataFrame = spark.sql(
      """
        | SELECT
        |     *
        | FROM temp
      """.stripMargin)


    frame1.show()

  }

  //  def sumName(requestmode: Int, processnode: Int): String = {
  //    if (1 == requestmode && 1 == processnode) {
  //
  //    }
  //
  //  }
}
