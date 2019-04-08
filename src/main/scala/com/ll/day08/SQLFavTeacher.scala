package com.ll.day08

import java.net.URL

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @Author liuliang
 * @Date   2019/4/6 0006 17:28
 */
object SQLFavTeacher {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("please ensure that you have 2 params (dataPath, topN(Integer))")
      System.exit(1)
    }

    val topN = args(1).toInt
    val spark = SparkSession.builder().appName("SQLFavTeacher").getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.read.textFile(args(0))
    val teacherData: Dataset[(String, String)] = lines.map(line => {
      val tIndex: Int = line.lastIndexOf("/") + 1
      val teacher: String = line.substring(tIndex)
      val host = new URL(line).getHost
      //学科的index
      val sIndex = host.indexOf(".")
      val subject = host.substring(0, sIndex)
      (subject, teacher)
    })
    val df: DataFrame = teacherData.toDF("subject","teacher")

    df.createTempView("v_favTeacher")

    val tmp1: DataFrame = spark.sql(s"select count(*) as counts, subject, teacher from v_favTeacher group by subject,teacher")

    tmp1.createTempView("v_tmp")

/// val temp2          = spark.sql(           s"SELECT *, row_number() over(order by counts desc) g_rk FROM (SELECT subject, teacher, counts, row_number() over(partition by subject order by counts desc) sub_rk FROM v_temp_sub_teacher_counts) temp2 WHERE sub_rk <= $topN")
//    val df2: DataFrame = spark.sql(s"select *,row_number() over(order by counts desc) as glo_rk from (select subject, teacher, counts,row_number() over(partition by subject order by counts desc) as sub_rk, row_number() over(order by counts desc) as g_rk from v_tmp) where sub_rk <= $topN")
    val df2: DataFrame = spark.sql(s"select *,dense_rank() over(order by counts desc) as glo_rk from (select subject, teacher, counts,row_number() over(partition by subject order by counts desc) as sub_rk, row_number() over(order by counts desc) as g_rk from v_tmp) where sub_rk <= $topN")
    //val df2: DataFrame = spark.sql(s"select *,row_number() over(partition by subject order by counts desc) as sub_rk, rank() over(order by counts desc) as g_rk from v_tmp where sub_rk <= $topN")


//    tmp1.show()

    df2.show()
    spark.close()

  }

}
