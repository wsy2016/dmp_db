package com.qianfen.spark

import com.qianfen.spark.beans.ScoreBean2
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.reflect.internal.util.TableDef.Column

/**
  *
  * Description: DateFrame 带有schme信息的rdd
  *
  * Author: wsy
  *
  * Date: 2019/4/1 20:58
  *
  */
object DataFrameDemo {


  private final val SUBJECT_PATH = "/Users/wensiyang/Downloads/dmp/resources/score.txt"
  private final val RESULT_PATH = "/Users/wensiyang/Downloads/dmp/result/dataFreameJdon"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[ScoreBean2] = sc.textFile(SUBJECT_PATH)
      .map(_.split(","))
      .filter(_.length >= 7)
      .map(arr => ScoreBean2(arr(0).toInt
        , arr(1).toInt
        , arr(2).toInt
        , arr(3).toInt
        , arr(4).toInt
        , arr(5)
        , arr(6)))

    //导入隐饰操作，否则RDD无法调用toDF方法
    import spark.implicits._
    val frame: DataFrame = rdd.toDF().cache()

    //val frame: DataFrame = spark.createDataFrame(rdd)
    //frame.show(10)
    //dsl
//    frame.select("*").show()
//    frame.select(frame("studentId") + 1).show()
//    frame.filter(frame("studentId") > 112).show()
//    frame.groupBy("studentId")

    //sql
    frame.createOrReplaceTempView("score")
    spark.sql("select *  from score").write.json(RESULT_PATH)

    sc.stop()
    spark.close()
  }
}