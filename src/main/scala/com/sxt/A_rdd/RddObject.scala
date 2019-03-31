package com.sxt.A_rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/3/19 21:53
  *
  */
object RddObject {

  val path = "/Users/wensiyang/Downloads/dmp/resources/people.txt"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()


    /**
      *
      *  - A list of partitions
      *  - A function for computing each split
      *  - A list of dependencies on other RDDs
      *  - Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
      *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
      * an HDFS file)
      *
      **/
    val rdd: RDD[String] = spark.sparkContext.textFile(path).map(_.replace(",", ":"))
    //rdd持久化在内存里面,下面要复用
    rdd.persist(StorageLevel.fromString("NONE"))
    val l: Long = rdd.filter(_.contains(".")).count() //一个action就是一个job
    val l2: Long = rdd.filter(_.contains("30")).count()
    println(l)
    println(l2)
  }

}
