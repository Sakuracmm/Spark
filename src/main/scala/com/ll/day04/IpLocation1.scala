package com.ll.day04

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author liuliang
 * @Date   2019/4/1 0001 21:18
 */
object IpLocation1 {

  def main(args: Array[String]): Unit = {

    if(args.length != 3){
      println("your params is illegal! check it with three number!(localRulesPath, logPath, outputPath)")
      System.exit(1)
    }
    var conf = new SparkConf().setAppName("Iplocation1")

    val sc = new SparkContext(conf)

    //在driver获取到全部的IP规则数据（全部的IP规则数据在某一台机器上，跟driver在同一台机器上）
    //全部的Ip规则在Driver端（在Driver的内存中）
    val rules: Array[(Long, Long, String)] = TestIp.readRules(args(0))

    //将driver端的数据广播到Executor中
    //调用sc上的广播方法
    //广播变量的引用还是在driver端
    val brodcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)

    //创建RDD，读取访问日志
    val accessLines: RDD[String] = sc.textFile(args(1))

    //这个函数是在哪一端定义的（还是在Driver端）
    var func = (line: String) => {
      val fields = line.split("[|]")
      val ip = fields(1)
      //将ip转换为十进制
      val ipNum: Long = TestIp.ip2Long(ip)
      //进行二分法查找，通过driver的引用获取到Executor中的广播变量
      //该函数中的代码是在Executor中被调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播规则了
      val rulesInExecutor: Array[(Long, Long, String)] = brodcastRef.value
      //查找
      var province = "未知地址"
      val index: Int = TestIp.binarySearch(rulesInExecutor,ipNum)
      if(index != -1){
        province = rulesInExecutor(index)._3
      }
      (province,1)
    }

    //整理数据
    val provinceAndOne: RDD[(String,Int)] = accessLines.map(func)

    //聚合
    //val sum = (x: Int, y:Int) => x+y
//    provinceAndOne.reduceByKey(_+_)
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey((x:Int, y:Int) => x+y)

    //将结果打印
    val r = reduced.collect()
    println(r.toBuffer)

    reduced.saveAsTextFile(args(2))

    sc.stop()

  }
}
