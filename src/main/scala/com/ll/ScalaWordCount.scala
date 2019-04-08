package com.ll

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author liuliang
 * @Date   2019/3/25 0025 14:41
 */
object ScalaWordCount {

  def main(args: Array[String]): Unit = {

    //创建spark配置,设置应用程序的名字
    val conf = new SparkConf().setAppName("ScalaWordCount")
    conf.setMaster("spark://master:7077")
//    conf.set("spark.testing.memory", "1073741824")
    //创建spark的执行入口
    val sc = new SparkContext(conf)
    //指定以后从哪里读取数据创建RDD(弹性分布式数据集)
//    val lines: RDD[String] = sc.textFile(args(0))
    val lines = sc.textFile("hdfs://master:9000/wordcount/input")
    //    val unit = lines.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_).sortBy(_._2,false).saveAsTextFile(args(1))



    //切分压平
    val words: RDD[String] = lines.flatMap(_.split(" "))

    //将单词和一组合
    val wordAndOne: RDD[(String, Int)] = words.map((_,1))

    //按key进行聚合
    val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //排序
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2,false)

    //将结果保存到hdfs中
//    sorted.saveAsTextFile(args(1))
    sorted.saveAsTextFile("hdfs://mster:9000/wordcount/output8");

    lines.mapPartitionsWithIndex((index,it) => {
      //拿到一个分区，然后通过分区再取里面的数据
      it.map(x => s"part: $index , ele: ${x}")
    })

    val func = (index: Int,it: Iterator[Int]) => {
      it.map(e => s"part: $index, ele: ${e}")
    }


    //释放资源
    sc.stop()

  }
}
