package com.dmp.reportold

import java.util.Properties

import cn.dmp.beans.Log
import com.dmp.utils.{ConfUtil, ReportUtil}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  *
  * Description: 媒体分析(App)报告
  *
  * Author: wsy
  *
  * Date: 2019/2/15 10:49
  *
  */
object kAppAnalyseReport {

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      println(
        """
          |com.dmp.reportold.AppReport <logDataPath> <appMappingPath> <outputPath>
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
    //val df1: DataFrame = spark.read.parquet(logDataPath)
    val url = ConfUtil.getString("jdbc.url")
    val tableName = ConfUtil.getString("jdbc.tableName")
    val user = ConfUtil.getString("jdbc.user")
    val password = ConfUtil.getString("jdbc.password")

    val connProp = new Properties()
    connProp.put("user", user)
    connProp.put("password", password)
    val df: DataFrame = spark.read.jdbc(url, tableName, connProp)
    //    df.createOrReplaceTempView("log")
    //
    //    val result = spark.sql(
    //      """
    //        |select
    //        |provincename, cityname,
    //        |sum(case when requestmode=1 then 1 else 0 end) `总请求`,
    //        |sum(case when requestmode=1 and processnode >=2 then 1 else 0 end) `有效请求`,
    //        |sum(case when requestmode=1 and processnode =3 then 1 else 0 end) `广告请求`,
    //        |sum(case when iseffective=1 and isbilling=1 and isbid=1 and adorderid !=0 then 1 else 0 end) `参与竞价数`,
    //        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1 else 0 end) `竞价成功数`,
    //        |sum(case when requestmode=2 and iseffective=1 then 1 else 0 end) `展示数`,
    //        |sum(case when requestmode=3 and iseffective=1 then 1 else 0 end) `点击数`,
    //        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*adpayment/1000 else 0 end) `广告成本`,
    //        |sum(case when iseffective=1 and isbilling=1 and iswin=1 then 1.0*winprice/1000 else 0 end) `广告消费`
    //        |from log
    //        |group by provincename, cityname
    //        |order by provincename
    //      """.stripMargin)
    // result.write.jdbc(url, "app_report",connProp)

    import spark.implicits._

    val rdds: Dataset[Log] = df.map(row => Log(row))
    //rdds.take(2).foreach(println(_))
    val value: Dataset[(String, List[Double])] = df.map(row => {
      val log: Log = Log(row)
      val adRequest = ReportUtil.calculateRequest(log)
      val adResponse = ReportUtil.calculateResponse(log)
      val adClick = ReportUtil.calculateShowClick(log)
      val adCost = ReportUtil.calculateAdCost(log)
      (log.appname, adCost ++ adResponse ++ adClick ++ adCost)
    })
    value.take(4).foreach(println(_))

    spark.stop()
  }

}
