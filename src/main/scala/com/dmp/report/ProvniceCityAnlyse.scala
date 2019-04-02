package com.dmp.report

import java.util.Properties

import com.dmp.utils.ConfUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Description: 各个省市数据分布情况,落地到MySQL;
  *
  * Author: wsy
  *
  * Date: 2019/4/2 09:35
  *
  */
object ProvniceCityAnlyse {

  private final val inputFile = "/Users/wensiyang/Downloads/dmp/result/dmpLogResult";

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(s"${this.getClass.getSimpleName}")
      .master("local[*]")
      .getOrCreate()
    val dataFrame: DataFrame = spark.read.parquet(inputFile)
    dataFrame.createOrReplaceTempView("logs")
    val url: String =ConfUtil.getString("")
    val table: String =""
    val connectionProperties: Properties = new Properties();
    spark
        .sql(
          """
            |select
            |   provincename,cityname, count(1) as cnt
            | from logs
            | group by provincename ,cityname
            | order by provincename
          """.stripMargin)
      .write.jdbc(url,table,connectionProperties)
    spark.close()

  }

}
