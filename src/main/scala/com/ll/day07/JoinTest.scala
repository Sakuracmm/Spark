package com.ll.day07

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object JoinTest {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("JoinTest").getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,laozhao,china","2,laoduan,korea","3,Alice,usa"))

    val lines2: Dataset[String] = spark.createDataset(List("china,beijing","usa,washington","japan,tokyo"))


    //对数据进行处理
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fields: Array[String] = line.split(",")
      val id = fields(0).toLong
      val name = fields(1).toString
      val nationCode = fields(2).toString
      (id, name, nationCode)
    })

    val tpDs2: Dataset[(String, String)] = lines2.map(line => {
      val fields: Array[String] = line.split(",")
      val contryName = fields(0)
      val capital = fields(1)
      (contryName, capital)
    })
    tpDs2

    val df1: DataFrame = tpDs.toDF("id","name","nation")
    val df2: DataFrame = tpDs2.toDF("contryName","capital")

    df1.createTempView("t_boy")
    df2.createTempView("t_contry")

    //right join
    //joinType Type of join to perform. Default `inner`. Must be one of:
    //`inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
    //                `right`, `right_outer`, `left_semi`, `left_anti`.
    val df3: DataFrame = df1.join(df2,$"nation" === $"contryName","right")
    //full outer join
//    val df4: DataFrame = df1.join(df2,Seq("nation","name"),"full_outer")
    val df4: DataFrame = df1.join(df2,$"nation" === $"contryName","full_outer")
    //inner join
    val df5: DataFrame = df1.join(df2,$"nation" === $"contryName")
    //cross join
    val df6: DataFrame = df1.crossJoin(df2)

    //left join
    val df7: DataFrame = spark.sql("select * from t_boy left join t_contry on t_boy.nation=t_contry.contryName")

    df1.show()
    df2.show()
    df3.show()
    println("-------------such as rightJoin------------")
    df4.show()
    println("-------------such as full outer join------------")
    df5.show()
    println("-------------such as innerJoin------------")
    df6.show()
    println("-------------such as crossJoin------------")
    df7.show()
    println("-------------such as leftJoin------------")



    spark.stop()

  }


}
