package com.ll.day08

import org.apache.spark.sql.{DataFrame, SparkSession}

object HiveOnSpark {

  def main(args: Array[String]): Unit = {
    //如果想让hive运行在spark上，一定要开启spark对hive的支持
    val spark = SparkSession.builder()
      .appName("HiveOnSpark")
      .enableHiveSupport()    //启用Spark对hive的支持（兼容hive的语法）
      .getOrCreate()

    //想要使用hive的元数据库，必须指定hive元数据库的位置?
    // 添加一个hive-site.xml到当前程序的classpath下即可

    //这里并没有创建t_boy这个表或视图？所以可以执行吗？
    val result: DataFrame = spark.sql("select * from t_boy order by fv desc")

    result.printSchema()

    result.explain()

    result.show()

    spark.close()

  }
}
