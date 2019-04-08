package com.ll.day07

import com.ll.day04.TestIp
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author liuliang
 * @Date   2019/4/1 0001 21:18
  * Join的代价太过昂贵，而且非常慢，解决思路是将小表（也就是ip规则表）在缓存起来（广播变量）
 */
object IpLocationSQL2 {

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
    val rulesDataset: Dataset[(Long, Long, String)] = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    //收集ip规则到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = rulesDataset.collect()
    //广播（必须使用sparkContext进行广播）
    //在Driver将广播变量的引用返回到Driver端
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)

    val accessLines: Dataset[String] = spark.read.textFile(args(1))

    //创建RDD，读取访问日志
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

    //定义一个函数，通过ip来找到对应的省份（UDF,类似于hive）
    //该函数的功能是输入一个IP地址对应的十进制，返回其对应的省份
    spark.udf.register("ip2province" ,(ipNum: Long) => {
      //查找IP规则，所以应该事先将IP规则广播，使之存在Executor中
      //函数的逻辑是在Executor中执行的，问题是怎样获取IP规则对应的数据？
      //利用广播变量的引用，就可以获得
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      //根据ipNum对应的十进制查找省份名字
      val i: Int = TestIp.binarySearch(ipRulesInExecutor,ipNum)
      var province ="未知IP地址"
      if(i != -1){
        province = ipRulesInExecutor(i)._3
      }
      province
    })

    //执行SQL
    val df3: DataFrame = spark.sql("select ip2province(ipNum) province, url from t_ipLog order by url desc")
    df3.show()

    spark.stop()

  }
}
