package com.ll.day04

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author liuliang
 * @Date   2019/4/1 0001 21:18
 */
object IpLocation3 {

  def main(args: Array[String]): Unit = {

    if(args.length != 2){
      println("your params is illegal! check it with three number!(rulesPath, logPath)")
      System.exit(1)
    }
    var conf = new SparkConf().setAppName("Iplocation1")

    val sc = new SparkContext(conf)

    //读取hdfs中的IP规则
    val rulesLines: RDD[String] = sc.textFile(args(0))
    //整理ip数据
    val ipRulesRDD: RDD[(Long, Long, String)] = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })

    //将分散在多个Executor中的部分ip规则收集到driver端
    val ruledInDriver: Array[(Long, Long, String)] = ipRulesRDD.collect()

    //将driver端的数据广播到Executor中
    //调用sc上的广播方法
    //广播变量的引用还是在driver端
    val brodcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(ruledInDriver)

    //创建RDD，读取访问日志
    val accessLines: RDD[String] = sc.textFile(args(1))

    //整理数据
    val provinceAndOne: RDD[(String,Int)] = accessLines.map(log => {

      //将log日志每一行进行切分
      val fields = log.split("[|]")
      val ip = fields(1)
      //将ip转换为十进制
      val ipNum: Long = TestIp.ip2Long(ip)
      //进行二分法查找，通过driver的引用获取到Executor中的广播变量
      //该函数中的代码是在Executor中被调用执行的，通过广播变量的引用，就可以拿到当前Executor中的广播规则了
      //Driver端广播变量的引用是怎么样跑到Executor中的呢？
      //task是在Driver端生成的，广播变量的引用是伴随着task发送到Executor中的
      val rulesInExecutor: Array[(Long, Long, String)] = brodcastRef.value
      //查找
      var province = "未知地址"
      val index: Int = TestIp.binarySearch(rulesInExecutor,ipNum)
      if(index != -1){
        province = rulesInExecutor(index)._3
      }
      (province,1)
    })

    //聚合
    //val sum = (x: Int, y:Int) => x+y
//    provinceAndOne.reduceByKey(_+_)
    val reduced: RDD[(String, Int)] = provinceAndOne.reduceByKey((x:Int, y:Int) => x+y)

    //将结果打印
//    val r = reduced.collect()
//    println(r.toBuffer)

    //存储到hdfs中指定文件夹下
    //reduced.saveAsTextFile(args(2))
    /**
    reduced.foreach(tp => {
      //将数据写入到MySQL中
      //问？在哪一端获取mysql的连接的？
      //在Executor中的Task获取的jdbc连接
      val conn: Connection = DriverManager.getConnection("jdbc://slave3:3306/bigdata?charatorEncoding=UTF-8","root","root")
      //写入大量数据的时候有没有问题？
      val pstm: PreparedStatement = conn.prepareStatement("...")
      pstm.setString(1, tp._1)
      pstm.setInt(2,tp._2)
      pstm.executeUpdate()
      pstm.close()
      conn.close()
    })**/

    //一次拿出一个分区(一个分区拿一个连接，可以将一个分区中的多条数据写完再释放jdbc连接，这样更节省资源)
    reduced.foreachPartition(it => {
      TestIp.data2MySQL(it)
    })
    sc.stop()
  }
}
