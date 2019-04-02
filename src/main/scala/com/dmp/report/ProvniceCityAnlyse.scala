package com.dmp.report

import java.util.Properties

import com.dmp.utils.ConfUtil
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

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
    val url: String = ConfUtil.getString("jdbc.url")
    val table: String = ConfUtil.getString("jdbc.report.pro.tableName")
    val user = ConfUtil.getString("jdbc.user")
    val password = ConfUtil.getString("jdbc.password")

    val connProp = new Properties()
    connProp.put("user", user)
    connProp.put("password", password)

    val dataFrame: DataFrame = spark.read.parquet(inputFile)

    //方案一 sql
    //    dataFrame.createOrReplaceTempView("logs")
    //    spark
    //      .sql(
    //        """
    //          |select
    //          |   provincename,cityname, count(1) as cnt
    //          | from logs
    //          | group by provincename ,cityname
    //          | order by provincename
    //        """.stripMargin)
    //      .write.jdbc(url, table, connProp)

    //方案二 core
    val sc: SparkContext = spark.sparkContext
    val lines: RDD[((String, String), Int)] = sc.textFile("/Users/wensiyang/Downloads/dmp/resources/dmpLog.txt")
      .map(x => {
        x.split(",")
          .map(y => {
            y.trim()
          })
      })
      .filter(_.length > 80)
      .map(arr => ((arr(24), arr(25)), 1))
    val list: List[Row] = lines
      .countByValue()
      .map(tuple => {
        val pro: String = tuple._1._1._1
        val city: String = tuple._1._1._2
        val cnt: Long = tuple._2.toLong
        Row(pro, city, cnt)
      }).toList
    val rows: RDD[Row] = sc.parallelize(list)
    val schema: StructType = StructType(List(
      StructField("provincename", StringType),
      StructField("cityname", StringType),
      StructField("cnt", LongType)

    ))
    val frame: DataFrame = spark.createDataFrame(rows, schema)
    frame.show()
    sc.stop()
    spark.close()

  }

}
