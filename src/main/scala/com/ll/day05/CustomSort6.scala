package com.ll.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort6 {

  def main(args: Array[String]): Unit = {

    var conf = new SparkConf().setAppName("CustomSort2")

    val sc = new SparkContext(conf)

    //排序规则：首先按照颜值的从高到低（降序）进行排序，如果颜值相等，再按照年龄的从低到高（升序）排序
    val users = Array("laoduan 30 99","laozhao 29 9999","laozhang 28 98","laoyang 19 9999")

    //将Driver端的数据并行化编程RDD
    val lines: RDD[String] = sc.parallelize(users)

    //切分整理数据
    val tpRDD: RDD[(String,Int,Int)] = lines.map(line => {
      val fields: Array[String] = line.split(" ")
      val name: String = fields(0)
      val age: Int = fields(1).toInt
      val fv: Int = fields(2).toInt
      (name, age, fv)
//      new User(name,age,fv)
    })
    //使用元组中内部定义的来进行排序，Int类型可以直接比较
    //充分利用元组的比较规则，元组比较规则：先比第一，相等再比第二个
    //Ordering[(Int,Int)]最终比较规则的样式
    //on[(String,Int,Int)]未比较之前的数据格式
    //(t => (-t._3,t._2))怎样的规则转换为想要的顺序
    implicit val rules: Ordering[(String, Int,Int)] = Ordering[(Int,Int)].on[(String,Int,Int)](t => (-t._3,t._2))
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => tp)

    val r = sorted.collect()
    println(r.toBuffer)

    sc.stop()
  }
}
