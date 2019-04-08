package com.ll.day06

import java.net.InetAddress

/**
 * @Author liuliang
 * @Date   2019/4/4 0004 9:02
 */
class Rules {
  val rulesMap = Map("hadoop" -> 1,"spark" -> 2)
}

class Rules2 extends Serializable {
  val rulesMap = Map("hadoop" -> 3,"spark" -> 4)
}
object Rules3 extends Serializable {
  val rulesMap = Map("hadoop" -> 5,"spark" -> 6)
}

//第三种方式，希望Rules在Executor中实现序列化，不通过网络传输，所以不需要序列化
object Rules4{
  val rulesMap = Map("hadoop" -> 6,"spark" -> 6)
  val hostname = InetAddress.getLocalHost.getHostName
  println(s"@@@@@@@@@@@@@@@************$hostname******************!!!!!!!!!!!!!!!!!")
}