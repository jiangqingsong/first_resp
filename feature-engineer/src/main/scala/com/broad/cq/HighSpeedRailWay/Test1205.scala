package com.broad.cq.HighSpeedRailWay

import java.text.SimpleDateFormat

import com.boyun.common.DateUtil

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Test1205 {
  def main(args: Array[String]): Unit = {
    val ints = Array(1,2,3,4,5)


    val pattern = "yyyy-MM-dd hh:mm:ss"
    val fm = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val date = fm.parse("2019-11-25 00:30:13")
    val date2 = fm.parse("2019-11-25 00:00:13")
    println((date.getTime - date2.getTime)/1000.0/60)

    val arr1 = new ArrayBuffer[Int]()
    arr1.append(1)
    arr1.append(2)

    println(arr1.slice(0,1))
    println("120" > "71")

    val map1 = new mutable.HashMap[String, Int]()
    //map1.+=(("A", 2))
    //map1.+=(("B", 1))
    //println(map1.toSeq.sortWith(_._2 > _._2).head._2)
    println("11".distinct)
    println("11".size)
    val ab = Array(1,2,3,4,5,6,7)
    val left = ab.slice(0, ab.length-1)
    val right = ab.tail
    println(left.mkString(","))
    println(right.mkString(","))
    println(left(1))
    println(DateUtil.getGapMinutes("yyyy-MM-dd hh:mm:ss", "2019-11-25 00:30:13", "2019-11-25 00:00:13"))

    println(DateUtil.getGapSeconds("yyyy-MM-dd hh:mm:ss", "2019-11-25 00:30:13", "2019-11-25 00:00:13"))
    val a11 = Array((3, "A", "B"), (1, "A1", "B2"), (2, "A5", "B4"))
    println(a11.sortWith(_._1 > _._1).head)

    val t0 = "2019-11-25 00:30:13"
    val t1 = DateUtil.addMulMinutes(t0, 5, "yyyy-MM-dd hh:mm:ss")
    println(t1)
    val gap1 = DateUtil.getGapSeconds("yyyy-MM-dd hh:mm:ss", "2019-11-25 00:00:53", "2019-11-25 00:00:13")
    val gap2 = DateUtil.getGapMinutes("yyyy-MM-dd hh:mm:ss", "2019-11-25 00:00:53", "2019-11-25 00:00:13")
    println(gap1)
    println(gap2)

  }
}
