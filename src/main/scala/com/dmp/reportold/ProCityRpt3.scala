package com.dmp.reportold

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Description:  利用spark算子实现-- 统计各个省,市流量情况
  *
  * Author: wsy
  *
  * Date: 2019/1/31 14:12
  *
  */
object ProCityRpt3 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    //lines.collect().foreach(println(_))

    val lines = sc.textFile("/Users/wensiyang/Downloads/resources/dmpLog.txt")
      .map(x => {
        x.split(",")
          .map(y => {
            y.trim()
          })
      })
      .filter(_.length > 80)

    /*
    *  统计各个省,市流量情况
    *
    *  Array=>((pri,city),1)=>((pri,city),n)
    *
    * */
    //lines.map(line => (((line(24), line(25)), 1))).reduceByKey(_ + _).collect().foreach(println(_))
    lines.map(line => (((line(24), line(25)), 1))).groupByKey().map(x=>{x._2}).collect().foreach(println(_))

    /*
    *
    * reduceByKey 将相同的key的value聚合成一个数组func计算
    *
    * */
    sc.stop()
  }

}
