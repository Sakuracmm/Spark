package com.ll.day07

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object JdbcDataSource {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JdbcDataSource").getOrCreate()

    import spark.implicits._

    //load这个方法会读取真正的mysql数据吗？ --> 并没有，只是先获取到表头信息
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://slave3:3306/userdb",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "dbtable" -> "emp",
        "user" -> "root",
        "password" -> "root")
    ).load()

    logs.printSchema()
    println("--------------表头信息---------------")

    logs.show()
    println("--------------数据信息----------------")

    val filtered: Dataset[Row] = logs.filter(r =>{
      r.getAs[Int]("id") <= 1206
    })

    filtered.show()
    println("------------------过滤后的信息------------")

    //lmabda表达式
    val r = logs.filter($"id" <= 1206)
    //val r = logs.where($"age" <= 13)

    val result: DataFrame = logs.select($"id", $"name", $"salary" * 10 as "totalSalary")
    result.show()
    println("-------------------自定义select信息-------------")

    val result2: DataFrame = logs.select($"name")
    //写入数据
//    val props = new Properties()
//    props.put("user","root")
//    props.put("password","root")
//    result.write.mode("ignore").jdbc("jdbc:mysql://slave3:3306/userdb", "emp_sparkSQL", props)




    //DataFrame保存成text时(只能保存一列,并且该列的数据必须是字符串)
    result2.write.text("hdfs://master:9000/sparkTest/text")

    result.write

    //保存为json文本
    //注意，不知道为什么，可以创建本地的文件夹，但是没有办法保存文件
    result.write.json("hdfs://master:9000/sparkTest/json")

    result.write.csv("hdfs://master:9000/sparkTest/csv")

    result.write.parquet("hdfs://master:9000/sparkTest/parquet")

    //reslut.show()

    spark.close()
  }



}
