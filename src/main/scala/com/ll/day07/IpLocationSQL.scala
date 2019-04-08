package com.ll.day07

import com.ll.day04.TestIp
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author liuliang
 * @Date   2019/4/1 0001 21:18
 */
object IpLocationSQL {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("your params is illegal! check it with three number!(rulesPath, logPath)")
      System.exit(1)
    }
    var conf = new SparkConf().setAppName("IplocationSQL")
    val sc = new SparkContext(conf)

    val spark = SparkSession.builder().appName("IplocationSQL").getOrCreate()

    //读取hdfs中的IP规则
    val rulesLines: Dataset[String] = spark.read.textFile(args(0))
    //整理ip数据
    import spark.implicits._
    val ipRulesDataSet: Dataset[(Long, Long, String)] = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })

    //将分散在多个Executor中的部分ip规则收集到driver端
    val rulesTable: DataFrame = ipRulesDataSet.toDF("startNum","endNum","province")
    rulesTable.createTempView("t_ipRules")



    //将driver端的数据广播到Executor中
    //调用sc上的广播方法
    //广播变量的引用还是在driver端
//    val brodcastRef: Broadcast[Dataset[(Long, Long, String)]] = sc.broadcast(ipRulesDataSet)

    //创建RDD，读取访问日志
    val accessLines: Dataset[String] = spark.read.textFile(args(1))
    val accessValue: Dataset[(Long, String)] = accessLines.map(log => {
      val fields = log.split("[|]")
      val ip: String = fields(1)
      val ipNum: Long = TestIp.ip2Long(ip)
      val ur: String = fields(2)
      (ipNum, ur)
    })
    accessValue

    val logTable: DataFrame = accessValue.toDF("ipNum","url")
    logTable.createTempView("t_ipLog")

    val df3: DataFrame = spark.sql("select * from t_ipRules r right join t_ipLog l on (l.ipNum between r.startNum and r.endNum)")

    df3.show(100)
    //整理数据
//    val provinceAndOne: RDD[(String,Int)] = accessLines.map(log => {
//
//      //将log日志每一行进行切分
//      val fields = log.split("[|]")
//      val ip = fields(1)
//      //将ip转换为十进制
//      val ipNum: Long = TestIp.ip2Long(ip)
//      //进行二分法查找，通过driver的引用获取到Executor中的广播变量
//      //该函数中的代码是在Executor中被调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播规则了
//      //Driver端广播变量的引用是怎么样跑到Executor中的呢？
//      //task是在Driver端生成的，广播变量的引用是伴随着task发送到Executor中的
//      val rulesInExecutor: Array[(Long, Long, String)] = brodcastRef.value
//      //查找
//      var province = "未知地址"
//      val index: Int = TestIp.binarySearch(rulesInExecutor,ipNum)
//      if(index != -1){
//        province = rulesInExecutor(index)._3
//      }
//      (province,1)
//    })

    //聚合
    //val sum = (x: Int, y:Int) => x+y
//    provinceAndOne.reduceByKey(_+_)
//    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey((x:Int, y:Int) => x+y)

    //将结果打印
//    val r = reduced.collect()
//    println(r.toBuffer)

//    reduced.saveAsTextFile(args(2))

    spark.stop()

  }
}
