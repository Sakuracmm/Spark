package com.ll.day09

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka.KafkaReceiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author liuliang
 * @Date   2019/4/8 0008 16:25
 */
object KafkaWordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("KafkaWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))

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
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)
    //打印结果（Action）
    reduced.print()
    //启动SparkStreaming程序
    ssc.start()
    //等待优雅退出
    ssc.awaitTermination()
  }
}
