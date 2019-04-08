package com.ll.day07

import java.lang

import org.apache.orc.impl.TreeReaderFactory.LongTreeReader
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

//自定义聚合函数
object UdafTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("UdafTest").getOrCreate()

    //生成一个1到10的数组,【）
    val range: Dataset[lang.Long] = spark.range(1,11)

    val geomean: GeoMean = new GeoMean

    //注册函数  --SQL风格
    spark.udf.register("gm",geomean)
    //将range注册成视图
    range.createTempView("v_range")
    val result: DataFrame = spark.sql("select gm(id) result from v_range")

    //DSL风格
    import spark.implicits._
    val result1: DataFrame = range.groupBy().agg(geomean($"id").as("geomean"))
    range.show()
    result.show()
    result1.show()
    result.explain()
    spark.stop()
  }

}

//重写8个方法
class GeoMean extends UserDefinedAggregateFunction{
  //输入数据的类型
  override def inputSchema: StructType = StructType(List(
    StructField("value", DoubleType)
    ))

  //产生中间结果的数据类型
  override def bufferSchema: StructType = StructType(List(

    StructField("product",DoubleType),   //分区运算过后产生的的中间结果
     StructField("counts",LongType)     //参与运算的个数
  ))

  //最终返回的结果类型
  override def dataType: DataType = DoubleType

  //确保一致性，一般用true
  override def deterministic: Boolean = true

  //指定初始值
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    //参与运算的初始值
    buffer(0) = 1.0
    //参与运算个数的初始值
    buffer(1) = 0L
  }

  //没有一条数据参与运算就更新一下中间结果（update相当于在每一个分区中运算）
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //参与运算的个数，也要更新
    buffer(1) = buffer.getLong(1) + 1L
    //每有一个数字参与运算就进行相乘，包含中间结果
    buffer(0) = buffer.getDouble(0) * input.getDouble(0)
  }

  //全局融合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    //每个分区参与运算的个数的中间结果进行相加
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
    //每个分区计算的结果进行相乘
    buffer1(0) = buffer1.getDouble(0) * buffer2.getDouble(0)
  }

  //计算最终的结果
  override def evaluate(buffer: Row): Double = {
    math.pow(buffer.getDouble(0), 1.toDouble / buffer.getLong(1))
  }
}
