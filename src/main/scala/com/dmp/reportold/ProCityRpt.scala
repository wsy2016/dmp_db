package com.dmp.reportold

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Description:  统计各个省,市流量情况
  *
  * Author: wsy
  *
  * Date: 2019/1/31 14:12
  *
  */
object ProCityRpt {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    val df = spark.read.parquet("/Users/wensiyang/Downloads/result/dmpLogResult")
    //df.show()
    df.createOrReplaceTempView("log")
    val rs: DataFrame = spark.sql("select provincename, cityname, count(*) ct from log group by provincename, cityname order by ct desc")
    val configuration: Configuration = sc.hadoopConfiguration
    val fileSystem = FileSystem.get(configuration)
    val path = new Path("/Users/wensiyang/Downloads/result/reportold.json")
    if (fileSystem.exists(path)) {
      fileSystem.delete(path, true)
    }

    rs.coalesce(2).write.json("/Users/wensiyang/Downloads/result/reportold.json")
    spark.stop()
  }

}
