package com.dmp.tools

import java.io.File

import com.dmp.utils.{NBUtil, SchemaUtils}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, _}


/**
  *
  * Description: 原始日志格式转化parquet才有snappy压缩
  *
  * http://spark.apache.org/docs/2.1.0/sql-programming-guide.html#parquet-files
  * Author: wsy
  *
  * Date: 2019/1/24 23:20
  *
  */
object BzipToParquet {


  /*
  *
  * 该类要在集群中跑批数据要有程序入口
  *
  * */
  def main(args: Array[String]): Unit = {

    //1.校验参数
    //        if (args.length != 3) {
    //          sys.exit
    //          println("参数异常!!!!!")
    //        } else {
    //          args.foreach(println(_))
    //        }

    /** *
      *
      * 2 创建spark
      */
    val spark = SparkSession
      .builder()
      .appName(s"${this.getClass.getSimpleName}")
      //rdd序列化到磁盘的格式
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec", "gzip")
      .master("local[*]")
      .getOrCreate()

    val sc: SparkContext = spark.sparkContext
    val sqlContext: SQLContext = spark.sqlContext


    /** *
      *
      * 3 读取日志根据业务需求对数据进行etl
      */
    val rdds: RDD[Array[String]] = sc.textFile("/Users/wensiyang/Downloads/resources/dmpLog.txt")
      .map(_.split(","))
      .filter(line => line.length > 80)

    /** *
      *
      * 4 转化为dataFrame
      */
    val row: RDD[Row] = rdds.map(arr => {
      Row(
        arr(0),
        NBUtil.toInt(arr(1)),
        NBUtil.toInt(arr(2)),
        NBUtil.toInt(arr(3)),
        NBUtil.toInt(arr(4)),
        arr(5),
        arr(6),
        NBUtil.toInt(arr(7)),
        NBUtil.toInt(arr(8)),
        NBUtil.toDouble(arr(9)),
        NBUtil.toDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        NBUtil.toInt(arr(17)),
        arr(18),
        arr(19),
        NBUtil.toInt(arr(20)),
        NBUtil.toInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        NBUtil.toInt(arr(26)),
        arr(27),
        NBUtil.toInt(arr(28)),
        arr(29),
        NBUtil.toInt(arr(30)),
        NBUtil.toInt(arr(31)),
        NBUtil.toInt(arr(32)),
        arr(33),
        NBUtil.toInt(arr(34)),
        NBUtil.toInt(arr(35)),
        NBUtil.toInt(arr(36)),
        arr(37),
        NBUtil.toInt(arr(38)),
        NBUtil.toInt(arr(39)),
        NBUtil.toDouble(arr(40)),
        NBUtil.toDouble(arr(41)),
        NBUtil.toInt(arr(42)),
        arr(43),
        NBUtil.toDouble(arr(44)),
        NBUtil.toDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        NBUtil.toInt(arr(57)),
        NBUtil.toDouble(arr(58)),
        NBUtil.toInt(arr(59)),
        NBUtil.toInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        NBUtil.toInt(arr(73)),
        NBUtil.toDouble(arr(74)),
        NBUtil.toDouble(arr(75)),
        NBUtil.toDouble(arr(76)),
        NBUtil.toDouble(arr(77)),
        NBUtil.toDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        NBUtil.toInt(arr(84))
      )
    })


    val df: DataFrame = spark.createDataFrame(row, SchemaUtils.logStructType)
    //    val view: Unit = df.createOrReplaceTempView("df")
    //    val view1: Unit = df.createOrReplaceTempView("df1")
    //    val sqlDf: DataFrame = spark.sql("Select * from df join df1 on df.sessionid =df1.sessionid")
    //    sqlDf.show()

    //df.show()


    /** *
      *
      * 4 落到到parquet
      */

    df.write.parquet("/Users/wensiyang/Downloads/result/dmpLogResult")
    sc.stop()

  }


}
