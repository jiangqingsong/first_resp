package com.brd.sdc.data_process

import com.brd.sdc.common.DateUtil.getPreMulHoursTime
import com.brd.sdc.common.SparkEnvUtil._

import scala.collection.mutable

/**
 * @author jiangqingsong
 * @description 异常流量分析
 * @date 2020-03-22 10:58
 */
object AbnormalTrafficAnalysis {

  import spark.implicits._

  /**
   *
   * @param day
   * @param minute
   * @param tabName
   * @param windowDay 样本时间窗口大小
   */
  def abnormalTrafficAnalysis(day: String, minute: String, tabName: String, windowDay: String = "7") {
    val startTime = getPreMulHoursTime(-24 * windowDay.toInt, "yyyyMMdd", day)
    val sampleDf = spark.sql(
      s"""
         |select starttime,utraffic,dtraffic from $tabName where day<=$day and day>$startTime
         |""".stripMargin).as[DpiFeature]

    val currentDf = spark.sql(
      s"""
         |select starttime,utraffic,dtraffic from $tabName where day=$day and minute=$minute
         |""".stripMargin).as[DpiFeature]
    currentDf.cache()

    val abnormalThresholdMap = sampleDf.rdd.filter(!_.starttime.isEmpty).map(l => {
      (l.starttime.substring(0, 13), l.utraffic.toInt + l.dtraffic.toInt)
    }).groupByKey().map(x => {
      val hour = x._1.substring(11, 13)
      val size = x._2.size
      val avgTraffic = x._2.sum.toDouble / size
      val sumStandardTraffic = if (size == 0) {
        0.0
      } else {
        x._2.map(y => (y - avgTraffic) * (y - avgTraffic)).reduce(_ + _)
      }
      val standartTraffic = math.sqrt(sumStandardTraffic / size)
      val uBound = standartTraffic + standartTraffic
      val dBound = standartTraffic - standartTraffic
      (hour, (dBound, uBound))
    }).collect().toMap

    val abnormalThresholdBC = spark.sparkContext.broadcast(abnormalThresholdMap)
    val sumTraffic = currentDf.rdd.map(x => x.dtraffic.toInt + x.utraffic.toInt).reduce(_ + _)
    val avgCurrentTraffic = sumTraffic.toDouble / currentDf.count()

    val avgCurrentTrafficBC = spark.sparkContext.broadcast(avgCurrentTraffic)

    val abnormalTrafficDf = currentDf.rdd.filter(!_.starttime.isEmpty).map(x => {
      val abnormalThresholdMap = abnormalThresholdBC.value
      val hour = x.starttime.substring(11, 13)
      val thresholdTuple = abnormalThresholdMap.get(hour).getOrElse((0.0, avgCurrentTrafficBC.value))
      val uBound = thresholdTuple._2
      val dBound = thresholdTuple._1
      val totalTraffic = x.utraffic.toDouble + x.dtraffic.toDouble
      val abnormalTraffic = if(totalTraffic > uBound || totalTraffic < dBound) {
        math.abs(totalTraffic - uBound)
      } else{
        0.0
      }
      (x.starttime, abnormalTraffic)
    }).reduceByKey(_ + _).map(x => {
      AbnormalTraffic(x._1, x._2.toString)
    }).toDF()

    abnormalTrafficDf.registerTempTable("ab_tmp")
    spark.sql(
      s"""
         |insert overwrite table sdc_detail.ppe_dpilog_abnormal partition(day=$day,minute=$minute)
         |select * from ab_tmp
         |""".stripMargin)

    spark.stop()
  }
}

case class AbnormalTraffic(
                            starttime: String,
                            abnormal_traffic: String
                          )
case class DpiFeature(
                       starttime: String,
                       utraffic: String,
                       dtraffic: String
                     )
