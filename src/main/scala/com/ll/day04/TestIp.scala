package com.ll.day04



import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

/**
 * @Author liuliang
 * @Date   2019/4/1 0001 20:13
 */
object TestIp {

  def ip2Long(ip:String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for(i <- 0 until fragments.length){
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def data2MySQL(it: Iterator[(String,Int)]):Unit = {
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://slave3:3306/bigdata?characterEncoding=UTF-8","root","root")
    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO access_log VALUES (?,?)")
    //将一个分区中的每一条数据拿出来
    it.foreach(tp => {
    pstm.setString(1,tp._1)
    pstm.setInt(2,tp._2)
    pstm.executeUpdate()
  })
    if (pstm != null) {
      pstm.close()
    }
    if(conn != null) {
      conn.close()
    }
  }

  def binarySearch(lines: Array[(Long,Long,String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while(low <= high){
      val middle = (low + high) / 2
      if((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if(ip < lines(middle)._1)
        high = middle - 1
      else{
        low = middle + 1
      }
    }
    -1
  }

  def readRules(path: String): Array[(Long,Long,String)] = {

    //读取ip规则
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    //对ip地址进行整理，并放入到内存中
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds: Array[String] = line.split("[|]")
      val startNum: Long = fileds(2).toLong
      val endNum: Long = fileds(3).toLong
      val province = fileds(6)
      (startNum, endNum, province)
    }).toArray
    rules
  }

  def main(args: Array[String]): Unit = {

    //    //数据实在内存中
    //    val rules: Array[(Long, Long, String)] = readRules("F:\\Big_Data\\小牛学堂-大数据24期-06-Spark安装部署到高级-10天\\spark-04-Spark案例讲解\\课件与代码\\课件与代码\\ip\\ip.txt")
    //
    //    //将ip地址转换为十进制
    //    val ipNum = ip2Long("192.186.43.42")
    //
    //    //查找
    //    val index: Int = binarySearch(rules, ipNum)
    //    if(index == -1){
    //      println("未知的 ip 地址！")
    //      System.exit(1)
    //    }
    //
    //    val province: String = rules(index)._3
    //    //根据脚标到rules中查找对应的数据
    //    println(province)
    //
    //
    //  }
  }
}
