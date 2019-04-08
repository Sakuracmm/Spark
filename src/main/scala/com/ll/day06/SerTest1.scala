package com.ll.day06

import java.net.InetAddress

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SerTest1 {

  def main(args: Array[String]): Unit = {
    //在Driver端被实例化
    //val rules = Rules

    //println("@@@@@@@@@@@@@@" + rules.toString + "@@@@@@@@@@@@@@@@@@")
    val conf = new SparkConf().setAppName("SerTest")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile(args(0))

    val r: RDD[(String,String,Int,String)] = lines.map(word => {
      //在map的函数中创建一个Rukes实例
      val rules = new Rules
      //函数的执行是在Executor中执行的（Task中执行的）
      val hostname = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      //Rules.rulesMap在哪一端被初始化？
      (hostname, threadName, rules.rulesMap.getOrElse(word, 0), rules.toString)
    })

    r.saveAsTextFile(args(1))
    sc.stop()
  }

}
