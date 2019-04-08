package com.ll.day06

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object DataSetWordCount {

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

    //使用DataSet的API(DSL)
    //val count: DataFrame = words.groupBy($"value" as "word").count().sort($"count" desc)

    //导入聚合函数
    import org.apache.spark.sql.functions._
    val counts = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)

    counts.show()
    spark.stop()
  }

}
