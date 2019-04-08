package com.ll.day05

/**
 * @Author liuliang
 * @Date   2019/4/2 0002 21:13
 */
object SortRules {
  implicit object OrderingXianRou extends Ordering[XianRou]{
    override def compare(x: XianRou, y: XianRou): Int = {
      if(x.fv == y.fv){
        x.age - y.age
      }else{
        y.fv - x.fv
      }
    }
  }
}
