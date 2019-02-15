package com.dmp.test

import java.util.Properties

import com.dmp.utils.ConfUtil
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  *
  * Description:spark 落地到MySQL
  *
  * Author: wsy
  *
  * Date: 2019/2/1 09:40
  *
  */
object DFToMysql {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(this.getClass.getSimpleName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val df: DataFrame = spark.read.parquet("/Users/wensiyang/Downloads/result/dmpLogResult")

    /*
    *
    *  注册一张临时表
    * */
    df.createOrReplaceTempView("log")
    //val rs: DataFrame = spark.sql("select provincename, cityname, count(*) ct from log group by provincename, cityname order by ct desc")
    val rs: DataFrame = spark.sql("select * from log")
    val url = ConfUtil.getString("jdbc.url")
    val tableName = ConfUtil.getString("jdbc.tableName")
    val user = ConfUtil.getString("jdbc.user")
    val password = ConfUtil.getString("jdbc.password")

    val connProp = new Properties()
    connProp.put("user", user)
    connProp.put("password", password)
    //rs.show()
    rs.write.mode(SaveMode.Append).jdbc(url, tableName, connProp)

    spark.stop()
  }

}
