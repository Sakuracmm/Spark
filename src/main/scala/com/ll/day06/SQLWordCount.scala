package com.ll.day06

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SQLWordCount {

  def main(args: Array[String]): Unit = {

    //创建SparkSession
    val spark = SparkSession.builder().appName("SQLWordCount").getOrCreate()

    //(指定目录中读取数据,lazy)

    //DataSet分布式数据集，是对RDD的进一步封装，是更加智能的RDD
    //DataSet只有一列。默认这列叫做value
    val lines: Dataset[String] = spark.read.textFile(args(0))

    //整理数据
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))

    //注册表
    words.createTempView("v_wc")

    //执行sql语句（Transformation，lazy）
    val result: DataFrame = spark.sql("select value,count(*) as counts from v_wc group by value Order By counts desc")

    result.show()

    spark.stop()
  }

}
