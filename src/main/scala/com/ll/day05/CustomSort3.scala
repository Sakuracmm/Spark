package com.ll.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CustomSort3 {

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

    //排序(传入了一个排序规则，不会改变数据格式或是内容，只会改变顺序)
    val sorted: RDD[(String,Int,Int)] = tpRDD.sortBy(tp => Man(tp._2,tp._3))

    val r = sorted.collect()

    println(r.toBuffer)

    sc.stop()
  }
}

//用样例类可以不用实现Serializable
case class Man(age: Int, fv: Int) extends Ordered[Man]{
  override def compare(that: Man): Int = {
    if(this.fv == that.fv){
      this.age - that.age
    }else{
      -(this.fv - that.fv)
    }
  }
}