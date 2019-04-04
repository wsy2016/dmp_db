package com.dmp.Tag

import cn.dmp.beans.Log
import com.dmp.beans.Logs
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * Description: 给数据打上标签
  *
  * Author: wsy
  *
  * Date: 2019/4/3 11:43
  *
  */
object TagsContext {


  private final val RESUREC_PATH: String = "/Users/wensiyang/Downloads/dmp/resources/dmpLog.txt"


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.classesToRegister", classOf[Log].getName)
      .appName(s"${this.getClass.getSimpleName}")
      .master("lolcal[*]")
      .enableHiveSupport()
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val arrRdd = sc.textFile(RESUREC_PATH)
      .map(line => line.split(",", -1))
      .map(arr => {
        val log: Log = Log(arr)
        val stringToInt: Map[String, Int] = Tags4Local.makeTags(log)


      })


  }

}
