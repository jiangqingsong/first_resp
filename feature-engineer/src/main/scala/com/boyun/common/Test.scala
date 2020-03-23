package com.boyun.common

import java.text.{ParsePosition, SimpleDateFormat}
import java.util.Calendar

import com.boyun.data.CsCalllogAcess.quantizeTime
import com.boyun.data.{CsCalllogAcess, RoamingFeature}
import org.apache.commons.lang.StringUtils

import scala.collection.mutable
import scala.util.matching.Regex

object Test {
  def main(args: Array[String]): Unit = {

    val pList = Array("13172946920","13172946920","15116548800","15116548800","15116548800","13922562926",
      "13642541897","18068193115","15575379111","13922562926","13172946920","18826602137","18153383505",
      "17137799381","13172946920","15116548800","18907476357","15116548800","18068193115","15575379111",
      "13172946920","13642541897","15601475007","15601475007","15116548800","13172946920","15116548800","17346911168",
      "13922562926","15116548800","13786492007","15116548800","15116548800","13642541897","15116548800", "1400002519530")
    pList.foreach(x => {
      if(RoamingFeature.disPhoneCat(x) != "m"){
        println("non phone: " + x)
      }
    })

    println(RoamingFeature.disPhoneCat("1400002519530"))
    println(RoamingFeature.disPhoneCat("10010"))
    println(RoamingFeature.isContainLetter("1400002519530"))
    println(RoamingFeature.isContainLetter("asdfsf11"))
    val str = null
    val map1 = new mutable.HashMap[String, Int]()
    map1.+=((null, 4))
    map1.+=(("A", 3))
    map1.+=(("B", 2))
    map1.+=(("C", 1))
    if(map1.contains(null)){
      map1.remove(null)
    }
    val imei = if(map1.size == 0) "-1" else map1.toArray.sortWith((a, b) => a._2 > b._2)(0)._1
    println(imei)
    println(HashUtil.getSha1(imei))

    println("20190909" > "20190908")
    println("0".toDouble == 0)

    val arrs = Array("2021-10-09 00:00:00-173334", "2019-10-10 00:00:00-173334", "2019-10-10:00 :00-173344")
    println(arrs.sortWith((a, b) => a < b).mkString(","))
    val a22 = Array(1,2,3,4,5,6)
    val a10 = a22.slice(0, a22.length-1)
    val a11 = a22.tail
    println(a10.zip(a11).mkString(","))
    val map2 = new mutable.HashMap[String, Int]()
    map2.+=(("1", 1))
    println(map2.contains(null))
    map2.remove(null)

    val te = Array[Int]().toIterable
    //println(te.sum)
    println(te.sum)
    val xx = "1234567"
    println(xx.substring(4, xx.length))

    val imei1 = "1"
    println(if(imei1.length >= 14) imei1.substring(0, 14)else imei1)

    val  phone = "02566071129"
    val  phone1 = "95236053"

    println(RegUntil.disPhoneCat(phone))
    println(RegUntil.disPhoneCat(phone1))

    val aa1 = Array(1,2,3,4,5,6)
    val aa2 = Array(2,3,4,5,6,7)
    //(sortedStarttimes.tail zip sortedEndTimes.slice(0, sortedEndTimes.length - 2))
    println((aa1.tail zip aa2.slice(0, aa2.length - 2)).mkString("|"))

    println(CsCalllogAcess.quantizeTime("2019-09-23 15:54:21-713453"))
    println("2019-09-23 15:54:24-964230".substring(8, 10))

    val sortedStarttimes = Array(
      "2019-09-23 15:54:24-964230",
      "2019-09-23 15:54:24-964230",//1
      "2019-09-24 15:54:25-964230",//2
      "2019-09-25 15:54:24-964230",//3
      "2019-09-26 15:54:24-964230" //4
    )
    val sortedEndTimes = Array(
      "2019-09-23 15:53:24-964230",//1
      "2019-09-23 15:53:24-964230",//2
      "2019-09-23 15:54:24-964230",//3
      "2019-09-23 15:54:24-964230",//4
      "2019-09-26 15:58:24-964230"
    )
    val call_gap_less2m = (sortedStarttimes.tail zip sortedEndTimes.slice(0, sortedEndTimes.length - 2))
      //(start,end)
      .filter(x => {
      val startDate = x._1.substring(8, 10)
      val endDate = x._2.substring(8, 10)
      if(startDate == endDate){//same date
        quantizeTime(x._1) - quantizeTime(x._2) < 1.0 /30
      }else if(startDate.toInt - endDate.toInt == 1){//相差一天
        (24 + quantizeTime(x._1) - quantizeTime(x._2)) < 1.0 /30
      }else{
        false
      }
    })
    println(call_gap_less2m.mkString("|"))

    println(StringUtils.split("a_b", "_").mkString("|"))
    println(Array(1, 2, 3, 4).mkString("2", "xxx", "4"))
    println(DateUtil.convertTimestamp2Date("1572944379066", "yyyy-MM-dd HH:mm:ss"))
    println(DateUtil.convertTimestamp2Date(("1572944379066".toLong - 3600000L).toString, "yyyy-MM-dd HH:mm:ss"))
    val time = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).parse("2019-11-05 16:59:39", new ParsePosition(0)).getTime()
    println(DateUtil.getPreMulHoursTime(1,"yyyy-MM-dd HH:mm:ss", "2019-11-05"))
    println(DateUtil.getPreMulHoursTime(-1,"yyyy-MM-dd HH:mm:ss", "2019-11-05 16:59:39"))
  }

}
