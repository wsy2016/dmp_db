package com.dmp.tools

import cn.dmp.beans.Log
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  *
  * Description: 日志转化为parquet 使用自定义类创建schema
  *
  * Author: wsy
  *
  * Date: 2019/1/31 09:34
  *
  */
object BzipToParquetV2 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[Log]))
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config(conf)
      // 注册自定义类的序列化方式
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
//
//    val lines: RDD[Array[String]] = sc.textFile("/Users/wensiyang/Downloads/resources/dmpLog.txt")
//      .map(_.split(","))
//      .filter(_.length > 80)

    // 读取日志文件
    val dataLog: RDD[Log] = sc.textFile("/Users/wensiyang/Downloads/resources/dmpLog.txt")
      .map(line => line.split(",", -1))
      .filter(_.length >= 85).map(arr => Log(arr))
    /*
    * 转化df
    *
    * */

    val df: DataFrame = spark.createDataFrame(dataLog)
    //val dataFrame2: DataFrame = sparkSql.createDataFrame(dataLog) 与上面一个对等
    df.show()
    //dataFrame2.show()
    df.write.partitionBy("provincename","cityname").parquet("/Users/wensiyang/Downloads/result/dmpLogResult")


  }

}
