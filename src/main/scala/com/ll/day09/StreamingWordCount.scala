package com.ll.day09

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    //离线任务是创建SparkContext,现在要实现实时计算，用StreamingContext
    var conf = new SparkConf().setAppName("StreamingWordCount")
    val sc = new SparkContext(conf)

    //Streaming 是对SparkContext的包装，包装了一层之后就增加了实时的功能
    //第二个参数是小批次产生的时间间隔
    val ssc = new StreamingContext(sc, Milliseconds(5000))

    //有了ssc(StreamingContext),就可以创建SparkStreaming的抽象了DStream
    //从一个socket端口中读取数据
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.234.12",8888)

    //对DStream进行操作，你操作这个抽象（代理，描述），就像操作一个本地集合一样
    //切分压平
    val words: DStream[String] = lines.flatMap(_.split(" "))

    //将单词和一组合到一起
    val wordAndOne: DStream[(String, Int)] = words.map((_,1))

    //聚合
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //打印结果(Action)
    reduced.print()

    //启动SparkStreaming程序
    ssc.start()

    //等待优雅地退出
    ssc.awaitTermination()

  }

}
