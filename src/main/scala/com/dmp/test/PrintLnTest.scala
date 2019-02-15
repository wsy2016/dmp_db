package com.dmp.test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/1/29 17:00
  *
  */
object PrintLnTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val rdds: RDD[String] = sc.textFile("/Users/wensiyang/Downloads/resources/dmpLog.txt")
    rdds.collect().foreach(println(_))
    rdds.take(2).foreach(println(_))

    var array = Array(1, 45, 6, 78)
    array.map(println(_))
    /*
    *  rdd只有在遇到Action如(foreach,collection,take)时才会开始遍历运算如L27,L28
    * */
    rdds.map(println(_))

  }

}
