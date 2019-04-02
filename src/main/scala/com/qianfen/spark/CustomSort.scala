package com.qianfen.spark

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * Description:自定义排序
  *
  * Author: wsy
  *
  * Date: 2019/3/31 16:53
  *
  */
object CustomSort {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    //name,age,faceValue
    val manINfo: RDD[(String, Int, Int)] = sc.parallelize(Array(("tom", 25, 90), ("jack", 12, 89), ("rose", 35, 45)))
    manINfo.sortBy(_._2).foreach(println(_))

    sc.stop()
    spark.close()
  }

}
