package com.ll.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author liuliang
 * @Date   2019/4/4 0004 21:15
 */
object SQLDemo3 {
  def main(args: Array[String]): Unit = {

    //提交的这个程序可以连接到Spark集群中
    val conf = new SparkConf().setAppName("SQLDemo1").setMaster("master")

    //创建SparkSQL的连接（程序执行的入口）
    val sc = new SparkContext(conf)
    //Spark不能创建特殊的RDD(DataFrame)
    //将SparkContext包装进而增强
    val sqlContext = new SQLContext(sc)
    //创建特殊的RDD(DataFrame),就是有schema信息的RDD

    //先有一个普通的RDD，然后再关联上schema信息，进而转换成DataFrame
    val lines = sc.textFile("hdfs://master:9000/sparkTest/person")
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
    schema

    //将RowRDD关联Schema
    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD,schema)

    //不使用SQL的方式就不用注册临时表
    val df1: DataFrame = bdf.select("name","age","fv")

    import sqlContext.implicits._
    val df2: Dataset[Row] = df1.orderBy($"fv" desc,$"age" asc)

    df1.show()

    df2.show()

    //释放资源
    sc.stop()
  }
}
