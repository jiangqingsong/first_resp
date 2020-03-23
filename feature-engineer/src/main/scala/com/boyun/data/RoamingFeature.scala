package com.boyun.data

import java.util.Calendar

import scala.math._
import com.boyun.common.EnvUtil._
import com.boyun.common.{DateUtil, HDFSUtil, HashUtil}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

/**
  * gen roaminglog bigtable.
  */
object RoamingFeature {

  val BSS_ROAMINGLOG = "bss.roaminglog"
  val IMA_ROAMINGLOG_TMP = "ima.roaminglog_local_temp"

  /**
    * get the last day log data as one day's samples.
    *
    * @param date
    * @param thresholdDate
    * @return
    */
  def getCurrentOneDayData(date: String, thresholdDate: String): Dataset[RoamingLog] = {
    val currentDs = if (date < thresholdDate) {
      getOneDayData(date, IMA_ROAMINGLOG_TMP).repartition(1000)
    } else {
      getOneDayData(date, BSS_ROAMINGLOG, false)
    }
    currentDs
  }

  def getMulDaysOriginData(date: String, days: String, thresholdDate: String): Dataset[RoamingLog] = {
    val startDate = DateUtil.dateAdd(date, Calendar.DAY_OF_YEAR, -days.toInt, "yyyyMMdd")
    val originDs = if (startDate < thresholdDate && date >= thresholdDate) {
      val imaDs = getMulDayOriginData(startDate, thresholdDate, IMA_ROAMINGLOG_TMP).repartition(1000)
      val bssDs = getMulDayOriginData(thresholdDate, date, BSS_ROAMINGLOG)
      imaDs.union(bssDs)
    } else if (startDate < thresholdDate && date < thresholdDate) {
      getMulDayOriginData(startDate, thresholdDate, IMA_ROAMINGLOG_TMP).repartition(1000)
    } else {
      getMulDayOriginData(startDate, date, BSS_ROAMINGLOG, false)
    }
    originDs
  }

  def getOneDayData(date: String, tabName: String, isTmp: Boolean = true): Dataset[RoamingLog] = {
    import spark.implicits._
    if (isTmp) {
      spark.sql(s"select * from $tabName where day = $date").withColumn("minute", lit("-1"))
        .as[RoamingLog]
    } else {
      spark.sql(s"select * from $tabName where day = $date").as[RoamingLog]
    }
  }

  def getMulDayOriginData(startDate: String, endDate: String, tabName: String, isTmp: Boolean = true): Dataset[RoamingLog] = {
    import spark.implicits._
    if (isTmp) {
      spark.sql(
        s"""
           |select * from $tabName where day > $startDate and day <= $endDate
       """.stripMargin).withColumn("minute", lit("-1")).as[RoamingLog]
    } else {
      spark.sql(
        s"""
           |select * from $tabName where day > $startDate and day <= $endDate
       """.stripMargin).as[RoamingLog]
    }

  }

  /**
    *
    * @param date sample date
    * @param days the size of window data
    */
  def dataProcee(date: String, days: String, thredsholdDate: String): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    import spark.implicits._
    //val tabName = "roaminglog"
    //val currentLogDf = spark.sql(s"select * from bss.$tabName where day = $date").as[RoamingLog]
    val currentLogDf = getCurrentOneDayData(date, thredsholdDate)
    val df = getMulDaysOriginData(date, days, thredsholdDate)

    //val startDate = DateUtil.dateAdd(date, Calendar.DAY_OF_YEAR, -days.toInt, "yyyyMMdd")
    //val df = spark.sql(s"select * from bss.$tabName where day > $startDate and day <= $date").as[RoamingLog]
    df.cache

    val callerDf = df.filter(_.record_type == "20")
    val calledDf = df.filter(_.record_type == "30")

    //1. CALLERDF
    val callerFeatsDf = callerDf.rdd.map(x => {
      (x.caller_isdn, x)
    }).groupByKey.map(x => {
      val phone = x._1
      //1、 caller count
      val callerCnt = x._2.size
      //2  每个拜访cell呼叫次数
      val cellCnt = x._2.map(_.cell).toSet.size
      val callPerCell = callerCnt.toDouble / cellCnt

      //3\每个拜访cellular呼叫次数
      val cellularCnt = x._2.map(_.cellularflag).toSet.size
      val callPerVcellular = if (cellularCnt == 0) 0 else callerCnt.toDouble / cellularCnt

      //4 每个拜访lac呼叫次数
      val lacCnt = x._2.map(_.lac).toSet.size
      val callPerLac = if (lacCnt == 0) 0 else callerCnt.toDouble / lacCnt

      //5 msc
      val mscCnt = x._2.map(_.msc).toSet.size
      val callPerMsc = if (mscCnt == 0) 0 else callerCnt.toDouble / mscCnt

      //6 roaming_cityid
      val callCityCnt = x._2.map(_.roaming_cityid).toSet.size
      val callPerCallCity = if (callCityCnt == 0) 0 else callerCnt.toDouble / callCityCnt

      //7 chargeunit
      val chargeunitLog = x._2.map(_.chargeunit.toDouble)
      val totalDura = if (chargeunitLog.size == 0) {
        0.0
      } else {
        chargeunitLog.reduce(_ + _)
      }

      //8 通话平均时长
      var duraAvg = if (callerCnt == 0) 0 else totalDura / callerCnt

      //9 呼叫手机次数
      val callPhoneLog = x._2.map(x => (x.called_isdn, x.chargeunit)).filter(c => disPhoneCat(c._1) == "m")
      val callPhoneCnt = callPhoneLog.size

      //10 呼叫手机通话总时长
      val phoneDuraSum = if (callPhoneCnt == 0) {
        0.0
      } else {
        callPhoneLog.map(_._2.toDouble).reduce((a, b) => a + b)
      }


      //11 呼叫手机通话平均时长
      val phoneDuraAvg = if (callPhoneLog == 0) 0 else phoneDuraSum / callPhoneCnt
      //val phoneDuraAvg = if(phoneDuraAvg_0.isNaN) 0 else phoneDuraAvg_0

      //12 通话时长短于2秒次数
      val duraLess2sCnt = x._2.map(_.chargeunit).filter(_.toDouble <= 2).size

      //13 通话时长短于2秒次数占通话次数比
      val dura2sRate = if (callerCnt == 0) 0 else duraLess2sCnt.toDouble / callerCnt

      //14 话时长2-10秒次数
      val dura2to10sCnt = chargeunitLog.filter(x => x > 2 && x <= 10).size
      //15 通话时长2-10秒次数占通话次数比
      val dura2to10sRate = if (callerCnt == 0) 0 else dura2to10sCnt.toDouble / callerCnt

      //16  呼叫手机率
      val callPhoneRate = if (callerCnt == 0) 0 else callPhoneCnt.toDouble / callerCnt

      //17 呼叫固话次数
      val callFixedLog = x._2.map(x => (x.called_isdn, x.chargeunit.toDouble)).filter(c => disPhoneCat(c._1) == "f")
      val callFixCnt = callFixedLog.size

      //18 呼叫固话通话总时长
      val fixedDuraSum = if (callFixCnt == 0) {
        0.0
      } else {
        callFixedLog.map(_._2.toDouble).reduce((a, b) => a + b)
      }

      //19 呼叫固话通话平均时长
      val fixedFuraAvg = if (callFixCnt == 0) -1 else fixedDuraSum / callFixCnt

      //20 呼叫固话率
      val callFixedRate = if (callerCnt == 0) 0 else callFixCnt.toDouble / callerCnt

      //21 呼叫其他次数
      val callOtherLog = x._2.map(x => (x.caller_isdn, x.chargeunit)).filter(c => disPhoneCat(c._1) == "o")
      val callOtherCnt = callOtherLog.size

      //22 呼叫其他通话总时长
      val otherDuraSum = if (callOtherCnt == 0) {
        0
      } else {
        callOtherLog.map(_._2.toDouble).reduce((a, b) => a + b)
      }


      //23 呼叫其他通话平均时长
      val otherFuraAvg = if (callOtherCnt == 0) -1 else otherDuraSum / callOtherCnt

      //24 对端不重复号码数量
      val calledDistinctCnt = x._2.map(_.called_isdn).toSet.size

      //25 平均每个号码呼叫次数
      val callCntPerPhone = if (calledDistinctCnt == 0) 0 else callerCnt.toDouble / calledDistinctCnt

      //26 对端不重复手机号码数量
      val calledDistinctPhoneCnt = x._2.map(_.called_isdn).filter(x => disPhoneCat(x) == "m").toSet.size

      //27 平均每个手机号码呼叫次数
      val calledCntPerPhone = if (calledDistinctPhoneCnt == 0) 0 else callPhoneCnt.toDouble / calledDistinctPhoneCnt

      //28 对端不重复手机号码占总不重复号码数量比
      val distinctPhoneRate = if (calledDistinctCnt == 0) 0 else calledDistinctPhoneCnt.toDouble / calledDistinctCnt

      //29 对端不重复固定号码数量
      val calledDistinctFixCnt = x._2.map(_.called_isdn).filter(x => disPhoneCat(x) == "f").toSet.size

      //30 平均每个固定号码呼叫次数
      val calledCntPerFix = if (calledDistinctFixCnt == 0) -1 else callFixCnt.toDouble / calledDistinctFixCnt
      //31对端不重复固定号码占总不重复号码数量比
      val distinctFixRate = if (calledDistinctCnt == 0) 0 else calledDistinctFixCnt.toDouble / calledDistinctCnt
      //32 对端不重复其他号码数量
      val calledDistinctOtherCnt = x._2.map(_.called_isdn).filter(x => disPhoneCat(x) == "o").toSet.size
      //33 平均每个固定号码呼叫次数
      val calledCntPerOther = if (calledDistinctOtherCnt == 0) -1 else callOtherCnt.toDouble / calledDistinctOtherCnt
      //34 对端不重复其他号码占总不重复号码数量比
      val distinctOtherRate = if (calledDistinctCnt == 0) 0 else calledDistinctOtherCnt.toDouble / calledDistinctCnt

      //日活相关
      val activeDates = x._2.map(_.call_date).toSet

      //34 活跃天数
      val activeDays = activeDates.size

      //35 活跃率
      val activeRate = activeDays / days.toDouble

      val callDate2cntMap = new mutable.HashMap[String, Int]()
      x._2.map(_.call_date).foreach(x => {
        callDate2cntMap.+=((x, callDate2cntMap.get(x).getOrElse(0) + 1))
      })
      val sortedCallDate2cnts = callDate2cntMap.toArray.sortWith((a, b) => a._2 > b._2)

      //36 日呼叫最大值
      val callMax = sortedCallDate2cnts.head._2

      //37 日呼叫最小值
      val callMin = if (activeDays == days.toInt) sortedCallDate2cnts.last._2 else 0
      //38 日呼叫标准差
      val sumDateCall = if (callDate2cntMap.size == 0) {
        0.0
      } else {
        callDate2cntMap.values.reduce(_ + _).toDouble
      }
      val dateCallAvg = sumDateCall / days.toInt


      val temp0sum = (0 - dateCallAvg) * (0 - dateCallAvg) //通话次数为0天的分子处理
      val sumStandard = if (callDate2cntMap.size == 0) {
        0.0
      } else {
        //计算标准差时需要考虑为通话次数为0的天数情况
        callDate2cntMap.values.map(x => (x - dateCallAvg) * ((x - dateCallAvg))).reduce(_ + _) + (days.toInt - callDate2cntMap.size) * temp0sum
      }

      val standardOfDateCall = math.sqrt(sumStandard / days.toInt)

      //39 对端归属城市数
      val calledCityData = x._2.map(_.called_city)
      val calledCityCnt = calledCityData.toSet.size

      val city2cntMap = new mutable.HashMap[String, Int]()
      //40 常呼叫城市
      calledCityData.map(x => (x, 1)).foreach(x => {
        if (!city2cntMap.contains(x._1)) {
          city2cntMap.+=((x._1, 1))
        } else {
          city2cntMap.+=((x._1, x._2 + 1))
        }
      })
      city2cntMap.remove(null)
      val commonCalledCity = city2cntMap.toArray.sortWith((a, b) => a._2 > b._2)(0)._1
      //commonCalledCity.map(x =>)
      //val comCalledCityHash = HashUtil.getSha1(commonCalledCity).substring(0, 11)

      val cityId2cntMap = new mutable.HashMap[String, Int]()
      //40.1 常呼叫城市ID
      val calledCityIdData = x._2.map(_.called_cityid.toString)
      calledCityIdData.foreach(x => {
        cityId2cntMap.+=((x, cityId2cntMap.get(x).getOrElse(0) + 1))
      })
      cityId2cntMap.remove(null)
      val commonCalledCityId = cityId2cntMap.toArray.sortWith((a, b) => a._2 > b._2)(0)._1


      val day2duraMap = new mutable.HashMap[String, mutable.ArrayBuffer[String]]()
      //41 日工作时长总和 112320 -> 11h 23m 20s
      x._2.map(a => (a.call_date, a.call_starttime)).foreach(b => {
        day2duraMap.+=((b._1, day2duraMap.get(b._1).getOrElse(new ArrayBuffer[String]()).+=(b._2)))
      })

      val date2duras = day2duraMap.map(c => {
        val formatStartTime = c._2.map(x => {
          x.substring(0, 2).toDouble + x.substring(2, 4).toDouble / 60 + x.substring(4, 6).toDouble / 3600
        })
        val dura = formatStartTime.max - formatStartTime.min
        (c._1, dura)
      })

      var totalAllDura = 0.0
      date2duras.map(_._2).foreach(x => {
        totalAllDura += x
      })

      //42 日工作时长最大值
      val maxDuraDate = date2duras.map(_._2).max

      //43 日工作时长最小值
      val minDuraDate = if (activeDays != days.toInt) 0 else date2duras.map(_._2).min

      val date2duraSum = if (date2duras.size == 0) {
        0.0
      } else {
        date2duras.map(_._2).reduce(_ + _)
      }
      val date2duraAvg = date2duraSum / days.toInt

      val temp0sum_1 = (0 - date2duraAvg) * (0 - date2duraAvg) //通话次数为0天的分子处理
      val standardOfDateSum = if (date2duras.size == 0) {
        0.0
      } else {
        date2duras.map(x => (x._2 - date2duraAvg) * (x._2 - date2duraAvg)).reduce(_ + _) + (days.toInt - date2duras.size) * temp0sum_1
      }
      val standardOfDateDura = math.sqrt(standardOfDateSum / days.toInt)

      //45 日工作每小时通话数
      val callerCntPerHour = if (totalAllDura == 0) 0 else callerCnt / totalAllDura
      //result
      CallerFeats(phone, callerCnt.toString, callPerCell.toString, callPerVcellular.toString, callPerLac.toString,
        callPerMsc.toString, callPerCallCity.toString, totalDura.toString, duraAvg.toString, callPhoneCnt.toString,
        phoneDuraSum.toString, transNaN(phoneDuraAvg).toString, duraLess2sCnt.toString, dura2sRate.toString, dura2to10sCnt.toString, dura2to10sRate.toString,
        callPhoneRate.toString, callFixCnt.toString, fixedDuraSum.toString, fixedFuraAvg.toString, callFixedRate.toString,
        callOtherCnt.toString, otherDuraSum.toString, otherFuraAvg.toString, calledDistinctCnt.toString, callCntPerPhone.toString,
        calledDistinctPhoneCnt.toString, calledCntPerPhone.toString, distinctPhoneRate.toString, calledDistinctFixCnt.toString,
        calledCntPerFix.toString, distinctFixRate.toString, calledDistinctOtherCnt.toString,
        calledCntPerOther.toString, distinctOtherRate.toString, activeDays.toString, activeRate.toString, callMax.toString,
        callMin.toString, standardOfDateCall.toString, calledCityCnt.toString,
        commonCalledCity, commonCalledCityId, totalAllDura.toString, maxDuraDate.toString, minDuraDate.toString,
        standardOfDateDura.toString, callerCntPerHour.toString)
    })
    val callerFeatsDs = callerFeatsDf.toDS

    //2. CALLED DF
    val calledFeatsDf = calledDf.rdd.map(x => {
      (x.called_isdn, x)
    }).groupByKey.map(x => {
      val phone = x._1
      //总被呼叫次数
      val calledCnt = x._2.size

      //总被呼叫通话时长
      var totalChargeunit = 0.0

      x._2.map(_.chargeunit.toDouble).foreach(x => {
        totalChargeunit += x
      })

      //平均被呼叫通话时长
      val chargeunitPer = if (calledCnt == 0) 0 else totalChargeunit / calledCnt

      //被手机呼叫次数
      val callerIsPhoneCnt = x._2.map(x => (x.chargeunit, x.caller_isdn)).filter(x => disPhoneCat(x._2) == "m").size
      //被手机呼叫率
      val callerIsPhoneRate = if (calledCnt == 0) 0 else callerIsPhoneCnt.toDouble / calledCnt

      //被固话呼叫次数
      val callerIsFixCnt = x._2.map(x => (x.chargeunit, x.caller_isdn)).filter(x => disPhoneCat(x._2) == "f").size
      //被固话呼叫率
      val callerIsFixRate = if (calledCnt == 0) 0 else callerIsFixCnt.toDouble / calledCnt

      //被其他呼叫次数
      val callerIsOtherCnt = x._2.map(x => (x.chargeunit, x.caller_isdn)).filter(x => disPhoneCat(x._2) == "o").size

      CalledFeats(phone, calledCnt.toString, totalChargeunit.toString, chargeunitPer.toString, callerIsPhoneCnt.toString, callerIsPhoneRate.toString,
        callerIsFixCnt.toString, callerIsFixRate.toString, callerIsOtherCnt.toString)
    })
    val calledFeatsDs = calledFeatsDf.toDS


    //3. GROUPFEATSDF
    val callerGroupDf = df.filter(_.record_type == "20")
      .select("caller_isdn", "imei", "cell", "cellularflag", "lac", "msc", "caller_province", "roaming_cityid", "roaming_city")
      .toDF("group_phone", "imei", "cell", "cellularflag", "lac", "msc", "province", "roaming_cityid", "roaming_city")
    //.as[CommonFeats]
    val calledGroupDf = df.filter(_.record_type == "30").filter(x => !x.called_isdn.isEmpty)
      .select("called_isdn", "imei", "cell", "cellularflag", "lac", "msc", "called_province", "roaming_cityid", "roaming_city")
      .toDF("group_phone", "imei", "cell", "cellularflag", "lac", "msc", "province", "roaming_cityid", "roaming_city")

    //.as[CommonFeats]
    val allGroupDf = callerGroupDf.union(calledGroupDf)

    val groupFeatureDs = allGroupDf.as[GroupFeature]
      .rdd
      .filter(x => !x.group_phone.isEmpty)
      .map(x => (x.group_phone, x))
      .groupByKey.map(x => {
      val phone = x._1

      val imeis = x._2.map(_.imei)
      //设备数量
      var imei_sum = imeis.toSet.size
      //常用设备
      val imei2cntMap = new mutable.HashMap[String, Int]()
      imeis.foreach(x => {
        imei2cntMap.+=((x, imei2cntMap.get(x).getOrElse(0) + 1))
      })

      imei2cntMap.remove(null)
      val com_imei = if (imei2cntMap.size == 0) "-1" else imei2cntMap.toArray.sortWith((a, b) => a._2 > b._2)(0)._1

      //拜访cell数量
      val vcell_sum = x._2.map(_.cell).toSet.size

      //拜访cellular数量
      val vcellular_sum = x._2.filter(x => x != null).map(_.cellularflag).toSet.size

      //拜访lac数量
      val vlac_sum = x._2.map(_.lac).toSet.size

      //拜访msc数量
      val vmsc_sum = x._2.map(_.msc).toSet.size
      //拜访市数量
      val vcity_sum = x._2.map(_.roaming_cityid).toSet.size
      //常在市
      val ciry2cntMap = new mutable.HashMap[String, Int]()
      x._2.map(_.roaming_cityid).filter(x => !x.isEmpty).foreach(x => {
        ciry2cntMap.+=((x, ciry2cntMap.get(x).getOrElse(0) + 1))
      })
      ciry2cntMap.remove(null)
      val com_vcity = ciry2cntMap.toArray.sortWith((a, b) => a._2 > b._2)(0)._1
      //拜访高危省次数
      val high_risk_prov = Array("广西", "广东", "云南", "贵州", "福建")
      var high_risk_prov_sum = 0
      x._2.map(_.province).foreach(x => {
        if (high_risk_prov.contains(x)) {
          high_risk_prov_sum += 1
        }
      })
      GroupFeature2(phone, imei_sum.toString, com_imei, vcell_sum.toString, vcellular_sum.toString, vlac_sum.toString,
        vmsc_sum.toString, vcity_sum.toString, com_vcity.toString, high_risk_prov_sum.toString)
    }).toDS


    //main key
    val callerPhoneDf = currentLogDf.filter(x => {
      disPhoneCat(x.caller_isdn) == "m"
    }).filter(_.record_type == "20").select("caller_isdn").toDF("phone")

    val calledPhoneDf = currentLogDf.filter(x => {
      disPhoneCat(x.called_isdn) == "m"
    }).filter(_.record_type == "30").select("called_isdn").toDF("phone")
    val allPhoneDf = callerPhoneDf.union(calledPhoneDf).distinct

    //final result
    val resultDf = allPhoneDf
      .join(callerFeatsDs, allPhoneDf("phone") === callerFeatsDs("callerPhone"), "left")
      .join(calledFeatsDs, allPhoneDf("phone") === calledFeatsDs("calledPhone"), "left")
      .join(groupFeatureDs, allPhoneDf("phone") === groupFeatureDs("group_phone"), "left")
      .drop("calledPhone").drop("callerPhone").drop("group_phone")
      .filter("call_sum != 0")
    val finalResultDf = resultDf.withColumn("called_per_call", resultDf("called_sum") * 1.0 / resultDf("call_sum"))
    //write hdfs
    val finalResultDfColumns = finalResultDf.columns
    print(
      s"""
         |debugInfos: ${finalResultDfColumns.mkString(",")}
      """.stripMargin)
    val savePath = "/user/dengshq/jqs/bss/roamingFeature." + date
    HDFSUtil.deleteFile(savePath)
    val finalResultRdd = finalResultDf.rdd.map(row => {
      (0 until (finalResultDfColumns.length)).map(i => {
        row.get(i)
      }).mkString(",")
    }).repartition(10)
    //resultDf.write.mode("overwrite").save(savePath)
    HDFSUtil.deleteFile(savePath)
    finalResultRdd.saveAsTextFile(savePath)
    spark.stop()
  }


  def isContainLetter(str: String): Boolean = {
    ("[a-zA-Z]".r findAllIn (str)).size != 0
  }

  /**
    * 正则区分： 手机/固话/其他
    *
    * @param isdn
    * @return
    */
  def disPhoneCat(isdn: String): String = {
    val mReg = new Regex("^(1[3-9][0-9])\\d{8}$")
    val f1Reg = new Regex("^0[1-9](\\d{1,2}\\-?)\\d{7,8}$")
    val f2Reg = new Regex("^[1-9]{1}[0-9]{5,8}$")

    isdn match {
      case a: String if (mReg.findAllIn(a).size == 1) => "m"
      case a: String if (f1Reg.findAllIn(a).size == 1 || f2Reg.findAllIn(a).size == 1) => "f"
      case _ => "o"
    }

  }

  def transNaN(d: Double): Double = {
    if (d.isNaN) 0 else d
  }

  /**
    * get caller dataframe and called dataframe to analysis case.
    *
    * @param date
    * @param days
    */
  def testSourceData(date: String, days: String): Unit = {
    Logger.getRootLogger.setLevel(Level.ERROR)
    import spark.implicits._
    val tabName = "roaminglog"

    val startDate = DateUtil.dateAdd(date, Calendar.DAY_OF_YEAR, -days.toInt, "yyyyMMdd")
    val df = spark.sql(s"select * from bss.$tabName where day > $startDate and day <= $date").as[RoamingLog]
    df.cache

    val callerDf = df.filter(_.record_type == "20")
    val calledDf = df.filter(_.record_type == "30")

    val callerPath = "/user/dengshq/tmp/jqs/bss/caller_test"
    val calledPath = "/user/dengshq/tmp/jqs/bss/called_test"
    callerDf.rdd.saveAsTextFile(callerPath)
    calledDf.rdd.saveAsTextFile(calledPath)
  }


}




