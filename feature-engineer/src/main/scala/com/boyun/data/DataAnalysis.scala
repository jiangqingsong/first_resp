package com.boyun.data

import java.util.Calendar

import com.boyun.common.{DateUtil, HDFSUtil}
import com.boyun.common.EnvUtil._
object DataAnalysis {
  def caseAnalysisi(date: String, days: String, phone: String): Unit ={
    import spark.implicits._
    val startDate = DateUtil.dateAdd(date, Calendar.DAY_OF_YEAR, -days.toInt, "yyyyMMdd")
    val path = "/user/dengshq/tmp/jqs/xdr/case1." + phone
    HDFSUtil.deleteFile(path)
    spark.sql(
      s"""
         |SELECT * FROM xdr.cs_calllog_acess
         |WHERE day > $startDate and day <= $date AND phone is not null AND rphone is not null
         |AND (hprovince=130 or hprovince is null) and (phone=$phone or rphone=$phone)
       """.stripMargin).rdd.repartition(1).saveAsTextFile(path)
    spark.stop()
  }
}
