package com.ll.day09

import org.apache.spark.{HashPartitioner, SparkConf}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @Author liuliang
 * @Date   2019/4/8 0008 19:31
  *      可以累加历史结果
 */
object StatefulKafkaWordCount {

  /**
    * iter中第一个参数是聚合的key，这里具体指的是单词
    *       第二个参数代表当前批次产生的该单词出现的次数，是局部聚合之后的列，因为有多个分区，分区一是2，分区二是3...
    *       第三个参数代表初始值或是累加的中间结果，这里表示该单词历史出现的记录，第一次的初始值为nil(没有)，有了之后就是some（Int）
    *
    */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
//    iter.map( t=>{
//      //元组第一个元素是key
//      //元组第二个元素表示key本次出现的次数累加上之前key出现的次数
////      (t._1,t._2.sum + t._3.getOrElse(0))
//    })
    iter.map{
      case (x,y,z) =>{
        (x, y.sum + z.getOrElse(0))
      }
    }
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCount")

    val ssc = new StreamingContext(conf, Seconds(5))
    //如果要实时更新历史数据（累加），那么就要保存到hdfs或是其他文件系统中
    ssc.checkpoint("hdfs://master:9000/sparkTest/StatefulKafkaWordCount/ckp/")

    //zookeeper地址
    val zkQuorum = "master:2181,slave1:2181,slave2:2181"
    //自定义分组名
    val groupId = "g1"
    //topic  一个topic对应几个线程
    val topic = Map[String, Int]("canlie" -> 1)


    //创建DStream,需要KafkaDStream
    val data: ReceiverInputDStream[(String, String)] = KafkaUtils.createStream(ssc,zkQuorum,groupId,topic)

    //对数据进行处理
    //kafka的receiveInputDStream[(String,String)]里面装的的是一个元组（key是往kafka里面写入的key,value是实际写入的内容）
    val lines: DStream[String] = data.map(_._2)
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))
    //单词和一组合到一起
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))
    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(updateFunc,new HashPartitioner(ssc.sparkContext.defaultParallelism),true)
    //打印结果（Action）
    reduced.print()
    //启动SparkStreaming程序
    ssc.start()
    //等待优雅退出
    ssc.awaitTermination()
  }
}
