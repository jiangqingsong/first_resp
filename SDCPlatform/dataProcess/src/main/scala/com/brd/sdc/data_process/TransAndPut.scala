package com.brd.sdc.data_process

import com.brd.sdc.common.SparkEnvUtil._

/**
 * 解析入库:
 * 1、dpi采集数据
 * 2、防火墙采集数据
 */
object TransAndPut {
  def transAndPut(tabName: String, date: String, separator: String): Unit ={
    tabName match {
      case "dpi" => transDpiAndPut(date, separator)
      case "firewall" => transFirewallAndPut(date, separator)
    }
  }
  def transDpiAndPut(date: String, separator: String): Unit ={
    import spark.implicits._
    val filePath = "/xxx/dpi" + date + "/*"
    val sourceRdd = spark.sparkContext.wholeTextFiles(filePath)
    val dpiDs = sourceRdd.map(_._2).map(line => {
      val cols = line.split(separator)
      Dpi(cols(0),cols(1),cols(2),cols(3),cols(4),cols(5),cols(6),cols(7),cols(8),cols(9),cols(10),cols(11),
        cols(12),cols(13),cols(14),cols(15),cols(16),cols(17),cols(18),cols(19),cols(20),cols(21),cols(22))
    }).toDF
    val distinctedDpiDf = dpiDs.distinct()
    distinctedDpiDf
    val tmpName = "tmp_tab"
    distinctedDpiDf.createGlobalTempView(tmpName)
    spark.sql(
      s"""
         |insert into ods_dpi partition(date=$date) select * from $tmpName
         |""".stripMargin)

    spark.stop()
  }
  def transFirewallAndPut(date: String, separator: String): Unit ={
    import spark.implicits._

    val filePath = "/xxx/firewall/" + date + "/*"
    val sourceRdd = spark.sparkContext.wholeTextFiles(filePath)
    //todo  替换成firewall的字段信息
    val dpiDf = sourceRdd.map(_._2).map(line => {
      val cols = line.split(separator)
      Dpi(cols(0),cols(1),cols(2),cols(3),cols(4),cols(5),cols(6),cols(7),cols(8),cols(9),cols(10),cols(11),
        cols(12),cols(13),cols(14),cols(15),cols(16),cols(17),cols(18),cols(19),cols(20),cols(21),cols(22))
    }).toDF()
    val distinctedFirewallDf= dpiDf.distinct()
    val tmpName = "tmp_tab"
    distinctedFirewallDf.createGlobalTempView(tmpName)
    //todo  如果按小时分区就需要加小时分区
    spark.sql(
      s"""
         |insert into ods_firewall partition(date=$date) select * from $tmpName
         |""".stripMargin)

    spark.stop()
  }
}
case class Dpi(
                srcIPType :String,
                dstIPType :String,
                l4proto :String,
                assetType :String,
                protocol :String,
                srcPort :String,
                dstPort :String,
                startTime :String,
                endTime :String,
                ulTraffic :String,
                dlTraffic :String,
                ulPacktes :String,
                dlPackets :String,
                srcIP :String,
                dstIP :String,
                devType :String,
                devName :String,
                softName :String,
                softVer :String,
                vendor :String,
                os :String,
                osVer :String,
                netType :String
              )

case class Firewall(

                   )