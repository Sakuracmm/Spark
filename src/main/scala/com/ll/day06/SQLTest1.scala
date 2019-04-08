package com.ll.day06

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

/**
 * @Author liuliang
 * @Date   2019/4/4 0004 22:22
 */
object SQLTest1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("SQLTest1")
    val sc = new SparkContext(conf)

    //spark2.x SQL的编程API(SparkSession)
    //是spark2.x的执行入口
    val session = SparkSession.builder()
      .appName("SQLTest1")
      .getOrCreate()

    //创建RDD
    val lines: RDD[String] = session.sparkContext.textFile("hdfs://master:9000/sparkTest/person")

    //将数据进行整理
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1).toString
      val age = fields(2).toInt
      val fv = fields(3).toDouble
      Row(id, name, age, fv)
    })

    //结构类型，其实就是表头用于描述DataFrame的
    val schema: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)
    ))

    val df: DataFrame = session.createDataFrame(rowRDD, schema)

    import session.implicits._
    val df2: Dataset[Row] = df.where($"fv" > 98).orderBy($"fv" desc, $"age" asc)

    df.show()
    df2.show()
    session.stop()


  }
}
