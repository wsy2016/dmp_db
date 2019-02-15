package com.dmp.test

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import com.dmp.beans.CaseTest
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  *
  * Description:
  *
  * Author: wsy
  *
  * Date: 2019/1/30 09:48
  *
  */
object RddToDFTest {


  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local[*]")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    val rdds: RDD[Array[String]] = sc.textFile("/Users/wensiyang/Downloads/resources/dmpLog.txt")
      .map(_.split(","))


    val df: DataFrame = duf2(rdds, spark)
    df.show()
    //df.select("session").show
    //df.select(df("session"),df("advertisersid")+1).show
    //df.selectExpr("session","advertisersid").show()
  }


  /**
    *
    * Description: toDF
    * Author: wsy
    * Date: 2019/1/30 09:51
    * Param: [rdds]
    * Return: java.lang.Object
    */
  def duf1(rdds: RDD[Array[String]], spark: SparkSession): DataFrame = {

    import spark.implicits._
    val df: DataFrame = rdds.map(line => (line(0), line(1))).toDF("session", "advertisersid")
    //rdds.map(line=>CaseTest(session = 1))
    return df;
  }

  /**
    *
    * Description: Row
    * Author: wsy
    * Date: 2019/1/30 09:51
    * Param: [rdds]
    * Return: java.lang.Object
    */
  def duf2(rdds: RDD[Array[String]], spark: SparkSession): DataFrame = {

    import spark.implicits._

    val value: RDD[Row] = rdds.map(line => Row(line(0),line(1)))
    val testSchema = StructType(Array(StructField("session", StringType, true), StructField("advertisersid", StringType, true)))
    val frame: DataFrame = spark.createDataFrame(value,testSchema)
    return frame;
  }
}
