package com.ll.day05

import java.sql.{Connection, DriverManager}

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}

object JdbcRddDemo {

  val getConn = () => {
    DriverManager.getConnection("jdbc:mysql://slave3:3306/bigdata?characterEncoding=utf8","root","root");
  }

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("JDBC")

    val sc: SparkContext = new SparkContext(conf)

    //创建RDD,这个RDD会记录以后从MySQL中读取数据
    //new 了RDD，里面没有真正要计算的数据，而是告诉这个RDD以后触发Action时到哪里读取数据
    var jdbcRDD: RDD[(String,Int)] = new JdbcRDD(
      sc,
      getConn,
      "Select * from access_log Where count >= ? and count <= ?",
      60,     //起始值
      2000,  //最大值
      2,    //分区数量
      //以什么样的形式返回读取到的结果
      rs => {
      val province = rs.getString(1)
      val count = rs.getInt(2)
      (province,count)
    }
    )
    val r: Array[(String, Int)] = jdbcRDD.collect()
    println(r.toBuffer)
  }
}
