package com.qianfen.spark

import com.qianfen.spark.beans.ScoreBean
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * Description: 每个学科下面的子学科topN
  *
  * Author: wsy
  *
  * Date: 2019/3/27 11:11
  *
  */
object SubjectTopN {

  private final val SUBJECT_PATH = "/Users/wensiyang/Downloads/dmp/resources/subject.txt"
  private final val SCORE_PATH = "/Users/wensiyang/Downloads/dmp/resources/score.txt"
  private final val RESULT_PATH = "/Users/wensiyang/Downloads/dmp/result/scoreResult"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName(s"${this.getClass.getSimpleName}")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    //    val rdd: RDD[(String, String)] = sc.textFile(SUBJECT_PATH)
    //      .map(lines => {
    //        val arrs: Array[String] = lines.split("\\/")
    //        (arrs(0), arrs(1))
    //      })


    val value: RDD[String] = sc.textFile(SCORE_PATH).cache()
    val sqlRdd = value.map(_.split(","))
      .filter(_.length >= 7)
      .map(arr => ScoreBean(arr))
    /**
      * 出每个系每个班前3名。
      **/
    val frame: DataFrame = spark.createDataFrame(sqlRdd)
    frame.createOrReplaceTempView("score")
    val score: DataFrame = spark.sql(
      """
        |select a.studentId,a.sumScore ,a.className,a.departmentName from
        | (select id,
        |    studentId,
        |    language+math+english as sumScore,
        |    className,departmentName
        |  from score) a
        |   left join
        |  (select id,
        |    studentId,
        |    language+math+english as sumScore,
        |    className,departmentName
        |  from score ) b
        |  on a.className = b.className and a.departmentName=b.departmentName and a.sumScore<b.sumScore
        |  group by a.departmentName,a.className,a.sumScore,a.studentId
        |  having count(1)<2
        |  ORDER by a.departmentName,a.className,a.sumScore desc
      """.stripMargin
    )
    // score.show()

    val scoreold: DataFrame = spark.sql(
      """
        |
        |select id,
        |    studentId,
        |    language+math+english as sumScore,
        |    className,departmentName
        |  from score
        |  order by departmentName,className,sumScore desc
        |
        |
      """.stripMargin)

    //scoreold.show()
    //scoreold.write.csv(RESULT_PATH)
    val scRdd: RDD[(String, (String, Int))] = value.map(_.split(","))
      .filter(_.length >= 7)
      .map(arr => {
        val studentId: String = arr(1)
        val sumScore: Int = arr(2).toInt + arr(3).toInt + arr(4).toInt
        val key: String = new StringBuffer().append(arr(6)).append(arr(5)).toString
        (key, (studentId, sumScore))
      })
    scRdd.foreach(println(_))
    scRdd.groupByKey().map(item => {
      val key: String = item._1
      val tuples: List[(String, Int)] = item._2.toList.sortWith(_._2<_._2).reverse.take(3)
      (key,tuples)

    }).foreach(println(_))


    //每系每班每科前3名
    val scRddTwo = value
      .map(_.split(","))
      .filter(_.length >= 7)
      .map(scoreInfo => (scoreInfo(1), scoreInfo(2).toInt, scoreInfo(3).toInt, scoreInfo(4).toInt, scoreInfo(5), scoreInfo(6)))
    val groupRdd: RDD[((String, String), Iterable[(String, Int, Int, Int, String, String)])] = scRddTwo.groupBy(item => (item._6, item._5))
    val result = groupRdd.map(subG => {
      val (departmentId, classId) = subG._1
      //语言前3
      val languageTopK = subG._2.toList.sortBy(_._2)(Ordering.Int.reverse).take(3).map(item => item._2 + "分:学号" + item._1)
      //数学前3
      val mathTopK = subG._2.toList.sortBy(_._3)(Ordering.Int.reverse).take(3).map(item => item._3 + "分:学号" + item._1)
      //外语前3
      val englishTopK = subG._2.toList.sortBy(_._4)(Ordering.Int.reverse).take(3).map(item => item._4 + "分:学号" + item._1)
      (departmentId, classId, Map("语文前3" -> languageTopK, "数学前3" -> mathTopK, "外语前3" -> englishTopK))
    })
    result.foreach(println(_))
    sc.stop()
    spark.close()

  }

}
