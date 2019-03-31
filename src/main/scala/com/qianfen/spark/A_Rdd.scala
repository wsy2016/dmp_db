package com.qianfen.spark

import cn.dmp.beans.Log
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/3/23 13:45
  *
  */
object A_Rdd {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    /**
      *
      *  - A list of partitions  没有rdd都有多个分区(并行计算,宽窄依赖)
      *  - A function for computing each split 每个rdd计算都是以分区为单位
      *  - A list of dependencies on other RDDs old rdd-->DAG-->new Rdd
      *  - Optionally, a Partitioner for key-value RDDs
      * (e.g. to say that the RDD is hash-partitioned)
      *
      * 对于key-value类型的RDD，存在一个Partitioner(划分器)。（也就是它形成宽窄依赖）。
      * （划分器目前有两种，一种是HashPatitioner，根据key的 hash值将RDD划分到不同的分区，
      * 另一种是RangePartitioner，根据key的范围值划分到不同分区）。
      * 对于key-value的RDD可指定一个partitioner，
      * 告诉它如何分片；常用的有hash，range,非key-value的value的RDD的partitioner为none
      *
      *
      *  - Optionally, a list of preferred locations to compute each split on (e.g. block locations for
      * an HDFS file)
      **/


    //val lines: RDD[String] = sc.textFile("/Users/wensiyang/Downloads/The_man_of_property.txt")
    val rdds: RDD[Int] = sc.parallelize(List(1, 24, 5, 6, 8, 10), 3)

    //元素*2并且排序
    val result = rdds.map(_ * 2).sortBy(x => x, false)
    //过滤
    rdds.filter(_ > 10)

    //元素以数组的方式打印出来
    //println(result.collect().toBuffer)

    //并集,交集,去重
    val oneList = sc.parallelize(List("a", "e", "c"))
    val twoList = sc.parallelize(List("a", "e", "f", "g"))
    val threeList = sc.parallelize(List("f", "g", "b"))
    //并集去重
    oneList.union(twoList).distinct
    //交集
    oneList.intersection(twoList)
    oneList.subtract(twoList)


    ////key value
    val kv1: RDD[(String, Int)] = sc.parallelize(List(("key1", 1), ("key2", 2), ("key3", 3),("key2",4),("key3",6)))
    val kv2: RDD[(String, Int)] = sc.parallelize(List(("key5", 5), ("key4", 4), ("key3", 5)))
    //交集
    //(kv1 join (kv2)).foreach(print(_))
    //(kv1 leftOuterJoin (kv2)).foreach(print(_))
    //(kv1 union kv2).foreach(print(_))

    //分别用reduceBykey,groupByKey 实现woredCount
    //kv1.reduceByKey(_+_).foreach(print(_))
    kv1.groupByKey().mapValues(_.sum).foreach(print(_))




  }

}
