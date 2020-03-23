package com.boyun.data

import java.util.Calendar

import com.boyun.common.EnvUtil._
import com.boyun.common.{DateUtil, HDFSUtil, RegUntil}
import org.apache.spark.sql.Dataset
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


/**
  * description: CsCalllogAcess 特征提取
  *
  * @author jiangqingsong
  *         2019-10-23
  **/
object CsCalllogAcess {
  val ORIGIN_TAB_NAME = "xdr.cs_calllog_acess"
  val ORIGIN_LOCAL_TAB_NAME = "ima.cs_calllog_acess_from_csv"

  def nullValueProcess(x: String, defaultValue: String): String = {
    if (x == null || x.isEmpty ) defaultValue else x
  }

  def getCurrentOneDayData(date: String, thresholdDate: String): Dataset[CsCallLog] = {
    val currentDs = if (date < thresholdDate) {
      getOneDayData(date, ORIGIN_LOCAL_TAB_NAME)
    } else {
      getOneDayData(date, ORIGIN_TAB_NAME, false)
    }
    currentDs
  }

  def getMulDaysOriginData(date: String, days: String, thresholdDate: String): Dataset[CsCallLog] = {
    val startDate = DateUtil.dateAdd(date, Calendar.DAY_OF_YEAR, -days.toInt, "yyyyMMdd")
    val originDs = if (startDate < thresholdDate && date >= thresholdDate) {
      val imaDs = getMulDayOriginData(startDate, thresholdDate, ORIGIN_LOCAL_TAB_NAME)
      val bssDs = getMulDayOriginData(thresholdDate, date, ORIGIN_TAB_NAME)
      imaDs.union(bssDs)
    } else if (startDate < thresholdDate && date < thresholdDate) {
      getMulDayOriginData(startDate, thresholdDate, ORIGIN_LOCAL_TAB_NAME)
    } else {
      getMulDayOriginData(startDate, date, ORIGIN_TAB_NAME, false)
    }
    originDs
  }

  def getOneDayData(date: String, tabName: String, isTmp: Boolean = true): Dataset[CsCallLog] = {
    import spark.implicits._
    if (isTmp) {
      spark.sql(s"select * from $tabName where day = $date").withColumn("minute", lit("-1"))
        .as[CsCallLog]
    } else {
      spark.sql(s"select * from $tabName where day = $date").as[CsCallLog]
    }
  }
  def getMulDayOriginData(startDate: String, endDate: String, tabName: String, isTmp: Boolean = true): Dataset[CsCallLog] = {
    import spark.implicits._
    if (isTmp) {
      spark.sql(
        s"""
           |select * from $tabName where day > $startDate and day <= $endDate
       """.stripMargin).withColumn("minute", lit("-1")).as[CsCallLog]
    } else {
      spark.sql(
        s"""
           |select * from $tabName where day > $startDate and day <= $endDate
       """.stripMargin).as[CsCallLog]
    }

  }

      /**
    * CsCalllogAcess 特征提取入口
    *
    * @param date
    */
  def extractFeatures(date: String, days: String, coalesceNum: String, thredsholdDate: String): Unit = {
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    //val df = getOriginDataSource(date, days).coalesce(coalesceNum.toInt)

    val df = getMulDaysOriginData(date, days, thredsholdDate).coalesce(coalesceNum.toInt)
    df.persist(StorageLevel.MEMORY_ONLY_SER)
    df.count()
    //action oper
    val callerDf = df.filter(_.subevttype == "65536").rdd.map(x => (x.phone, x))
    val calledDf = df.filter(_.subevttype == "65538").filter(x => !x.phone.startsWith("10010")).rdd.map(x => (x.phone, x))

    //caller feature 1
    val callerFeatDs = callerDf.filter(x => x._1 != null && !x._1.isEmpty && !"null".equals(x._1) && !"NULL".equals(x._1))
      .groupByKey.map(x => {
      val phone_k = x._1
      val df = x._2

      //1.总呼叫次数
      val call_sum = df.size.toDouble
      //2.通话时长
      val talklens = df.map(_.talklen).map(x => nullValueProcess(x, "0").toDouble)
      val dura_sum = if (talklens.size == 0) {
        0.0
      } else {
        talklens.reduce(_ + _)
      }
      //3.通话平均时长
      val dura_avg = if (call_sum == 0) 0 else dura_sum / call_sum
      //4.本地用户
      val roamtype1 = df.map(_.roamtype).filter(x => "1".equals(x)).size
      //5.省内漫游L
      val roamtype2 = df.map(_.roamtype).filter(x => "2".equals(x)).size
      //6.呼叫接通次数
      val call_con_sum = df.map(_.talklen).map(x => nullValueProcess(x, "0").toDouble).filter(x => x != 0).size
      //7.呼叫接通率
      val call_con_rate = call_con_sum / call_sum
      //8.呼叫未接通次数
      val call_uncon_sum = call_sum - call_con_sum
      //9.用户原因呼叫未接通次数
      val call_uncon_user_sum = df.map(_.result).filter(x => x == "1").size
      //10.用户原因呼叫未接通率
      val call_uncon_user_rate = if (call_uncon_sum == 0) "null" else (call_uncon_user_sum / call_uncon_sum.toDouble).toString
      //11.网络原因呼叫未接通次数
      val call_uncon_net_sum = df.map(_.result).filter(x => x == "2").size
      //12.网络原因呼叫未接通率
      val call_uncon_net_rate = if (call_uncon_sum == 0) "null" else (call_uncon_net_sum / call_uncon_sum.toDouble).toString
      //13.其他原因呼叫未接通次数
      val call_uncon_other_sum = call_uncon_sum - call_uncon_user_sum - call_uncon_net_sum
      //14. 呼叫手机次数
      val call_m_logs = df.map(x => (x.rphone, nullValueProcess(x.talklen, "0").toDouble)).filter(c => RegUntil.disPhoneCat(c._1) == "m")
      val call_m_sum = call_m_logs.size.toDouble
      //15.呼叫手机通话总时长
      val dura_m_sum = if (call_m_sum == 0) {
        0.0
      } else {
        call_m_logs.map(_._2.toDouble).reduce(_ + _)
      }

      //16.呼叫手机通话平均时长
      val dura_m_avg = if (call_m_sum == 0) "null" else (dura_m_sum / call_m_sum.toDouble).toString
      //17.呼叫手机率
      val call_m_rate = call_m_sum / call_sum
      //18.呼叫固话次数
      val call_f_logs = df.map(x => (x.rphone, nullValueProcess(x.talklen, "0").toDouble)).filter(c => RegUntil.disPhoneCat(c._1) == "f")
      val call_f_sum = call_f_logs.size
      //19.呼叫固话通话总时长
      val dura_f_sum = if (call_f_sum == 0) {
        0.0
      } else {
        call_f_logs.map(_._2.toDouble).reduce(_ + _)
      }
      //20.呼叫固话通话平均时长
      val dura_f_avg = if (call_f_sum == 0) "null" else (dura_f_sum / call_f_sum.toDouble).toString
      //21.呼叫固话率
      val call_f_rate = call_f_sum / call_sum

      //22.呼叫其他次数
      val call_o_sum = call_sum - call_m_sum - call_f_sum
      //23.呼叫其他次数
      val call_o_logs = df.map(x => (x.rphone, nullValueProcess(x.talklen, "0").toDouble)).filter(c => RegUntil.disPhoneCat(c._1) == "o")
      //24.呼叫其他通话总时长
      val dura_o_sum = if (call_o_sum == 0) {
        0.0
      } else {
        call_o_logs.map(_._2.toDouble).reduce(_ + _)
      }

      //25.呼叫其他通话平均时长
      val dura_o_avg = if (call_o_sum == 0) "null" else (dura_o_sum / call_o_sum.toDouble).toString
      //26.对端不重复号码数量
      val opp_phone_sum = df.map(_.rphone).toSet.size
      //27.对端号码标准差
      val phone2cntMap = new mutable.HashMap[String, Int]()
      df.map(_.rphone).foreach(p => {
        phone2cntMap.+=((p, phone2cntMap.get(p).getOrElse(0) + 1))
      })
      val call_rphone_avg = call_sum / opp_phone_sum.toDouble
      val sumRphoneCntStandard = if (phone2cntMap.size == 0) {
        0.0
      } else {
        phone2cntMap.values.map(x => (x - call_rphone_avg) * ((x - call_rphone_avg))).reduce(_ + _)
      }
      val opp_phone_std = math.sqrt(sumRphoneCntStandard / opp_phone_sum.toDouble)

      //28.平均每个号码呼叫次数
      val call_per_phone = call_sum / opp_phone_sum.toDouble
      //29.对端不重复手机号码数量
      val opp_m_phone_sum = df.map(_.rphone).filter(c => RegUntil.disPhoneCat(c) == "m").toSet.size
      //30.平均每个手机号码呼叫次数
      val call_per_m_phone = if (opp_m_phone_sum == 0) "null" else (call_m_sum / opp_m_phone_sum.toDouble).toString
      //31.对端不重复手机号码占总不重复号码数量比
      val opp_m_phone_rate = if (opp_phone_sum == 0) 0.0 else opp_m_phone_sum / opp_phone_sum.toDouble
      //32.对端不重复固定号码数量
      val opp_f_phone_sum = df.map(_.rphone).filter(c => RegUntil.disPhoneCat(c) == "f").toSet.size
      //33.平均每个固定号码呼叫次数
      val call_per_f_phone = if (opp_f_phone_sum == 0) "null" else (call_f_sum / opp_f_phone_sum.toDouble).toString
      //34.对端不重复固定号码占总不重复号码数量比
      val opp_f_phone_rate = if (opp_phone_sum == 0) 0.0 else opp_f_phone_sum / opp_phone_sum.toDouble
      //35.对端不重复其他号码数量
      val opp_o_phone_sum = df.map(_.rphone).filter(c => RegUntil.disPhoneCat(c) == "o").toSet.size
      //36.平均每个固定号码呼叫次数
      val call_per_o_phone = if (opp_o_phone_sum == 0) "null" else (call_o_sum / opp_o_phone_sum).toString
      //37.对端不重复其他号码占总不重复号码数量比
      val opp_o_phone_rate = if (opp_phone_sum == 0) 0.0 else opp_o_phone_sum / opp_phone_sum.toDouble
      //38.本端释放数量
      val rel_b_sum = df.map(_.reldirection).filter(x => x == "1").size
      //39.本端释放占比
      val rel_b_rate = rel_b_sum / call_sum
      //40.本端手机释放数量
      val rel_b_m_sum = df.map(x => (x.rphone, x.reldirection)).filter(x => {
        x._2 == "1" && RegUntil.disPhoneCat(x._1) == "m"
      }).size
      //41.本端手机释放占比
      val rel_b_m_rate = if (call_m_sum == 0) "null" else (rel_b_m_sum / call_m_sum).toString
      //42.对端释放数量
      val rel_r_sum = df.map(_.reldirection).filter(x => x == "2").size
      //43.对端手机释放数量
      val rel_r_m_sum = df.map(x => (x.rphone, x.reldirection)).filter(x => {
        x._2 == "2" && RegUntil.disPhoneCat(x._1) == "m"
      }).size


      //part2
      //1.通话时长短于2秒次数
      val dura_2s = df.map(_.talklen).map(x => nullValueProcess(x, "0").toDouble).filter(x => x > 0 && x <= 2000).size
      //2.通话时长短于2秒次数占通话次数比
      val dura_2s_rate = dura_2s / call_sum.toDouble
      //3.通话时长2-10秒次数
      val dura_3to10s = df.map(_.talklen).map(x => nullValueProcess(x, "0").toDouble).filter(x => x > 2000 && x <= 10000).size
      //4.通话时长2-10秒次数占通话次数比
      val dura_3to10s_rate = dura_3to10s / call_sum.toDouble
      //5.上次通话结束至下次通话开始时间间隔短于2分钟的次数
      val sortedStarttimes = df.map(_.starttime).toArray.sortWith((a, b) => a < b)
      val sortedEndTimes = df.map(_.endtime).toArray.sortWith((a, b) => a < b)
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
      }).size
      /*.map(x => quantizeTime(x._1) - quantizeTime(x._2))
      .filter(x => x < 1.0 / 30).size*/

      //6.每个拜访lai呼叫次数
      val vlai_sum = df.map(_.lai).toSet.size
      val call_per_vlai = if (vlai_sum == 0) 0.0 else call_sum / vlai_sum
      //7.每个拜访cgi呼叫次数
      val cgi_sum = df.map(_.cgi).toSet.size
      val call_per_ = if (cgi_sum == 0) 0.0 else call_sum / cgi_sum
      //8.每个拜访msc呼叫次数
      val vmsc_sum = df.map(_.msc).toSet.size
      val call_per_vmsc = if (vmsc_sum == 0) 0.0 else call_sum / vmsc_sum
      //9.每个拜访mscip呼叫次数
      val vmscip_sum = df.map(_.mscip).toSet.size
      val call_per_vmscip = if (vmscip_sum == 0) 0.0 else call_sum / vmscip_sum
      //10.每个拜访市呼叫次数
      val vcity_sum = df.map(_.vcity).toSet.size
      val call_per_vcity = if (vcity_sum == 0) 0.0 else call_sum / vcity_sum
      //11.
      val vcountry_sum = df.map(_.vcountry).toSet.size
      val call_per_vcountry = if (vcountry_sum == 0) 0.0 else call_sum / vcountry_sum

      //12.活跃天数 
      val acitve_days = df.map(_.day).toSet.size
      //13.活跃率
      val acitve_rate = acitve_days / days.toDouble
      //14.日呼叫最大值
      val callDate2cntMap = new mutable.HashMap[String, Int]()
      df.map(_.day).foreach(x => {
        callDate2cntMap.+=((x, callDate2cntMap.get(x).getOrElse(0) + 1))
      })
      val sortedCallDate2cnts = callDate2cntMap.toArray.sortWith((a, b) => a._2 > b._2)
      val call_max = sortedCallDate2cnts.head._2
      //15.日呼叫最小值
      val call_min = if (acitve_days != days.toDouble) 0 else sortedCallDate2cnts.last._2
      //16.日呼叫标准差
      val dateCallAvg = call_sum / days.toDouble
      val temp0sum = (0 - dateCallAvg) * (0 - dateCallAvg) //通话次数为0天的分子处理
      val sumStandard = if (callDate2cntMap.size == 0) {
        0.0
      } else {
        //计算标准差时需要考虑为通话次数为0的天数情况
        callDate2cntMap.values.map(x => (x - dateCallAvg) * ((x - dateCallAvg))).reduce(_ + _) + (days.toInt - callDate2cntMap.size) * temp0sum
      }
      val call_std = math.sqrt(sumStandard / days.toDouble)

      //17.日工作时长总和
      val day2duraMap = new mutable.HashMap[String, mutable.ArrayBuffer[String]]()
      df.map(a => (a.day, a.starttime)).foreach(b => {
        day2duraMap.+=((b._1, day2duraMap.get(b._1).getOrElse(new ArrayBuffer[String]()).+=(b._2)))
      })

      val date2duras = day2duraMap.map(c => {
        val formatStartTime = c._2.map(x => {
          //eg: 2019-07-24 23:59:23-524571
          quantizeTime(x)
        })
        val dura = formatStartTime.max - formatStartTime.min
        (c._1, dura)
      })
      val worktime_sum = if (date2duras.size == 0) {
        0.0
      } else {
        date2duras.map(_._2).reduce(_ + _)
      }
      //18.日工作时长最大值
      val worktime_max = date2duras.map(_._2).max
      //19.日工作时长最小值
      val worktime_min = if (acitve_days != days.toInt) 0 else date2duras.map(_._2).min
      //20.日工作时长标准差
      val date2duraAvg = worktime_sum / days.toInt

      val temp0sum_1 = (0 - date2duraAvg) * (0 - date2duraAvg) //通话次数为0天的分子处理
      val standardOfDateSum = if (date2duras.size == 0) {
        0.0
      } else {
        date2duras.map(x => (x._2 - date2duraAvg) * (x._2 - date2duraAvg)).reduce(_ + _) + (days.toInt - date2duras.size) * temp0sum_1
      }
      val worktime_std = math.sqrt(standardOfDateSum / days.toInt)
      //21.日工作每小时通话数
      val call_per_workhour = if (worktime_sum == 0) 0 else call_sum / worktime_sum
      CsCallerFeature(
        phone_k, call_sum.toString, dura_sum.toString, dura_avg.toString, roamtype1.toString, roamtype2.toString, call_con_sum.toString,
        call_con_rate.toString, call_uncon_sum.toString, call_uncon_user_sum.toString, call_uncon_user_rate.toString,
        call_uncon_net_sum.toString, call_uncon_net_rate.toString, call_uncon_other_sum.toString, call_m_sum.toString,
        dura_m_sum.toString, dura_m_avg.toString, call_m_rate.toString, call_f_sum.toString, dura_f_sum.toString,
        dura_f_avg.toString, call_f_rate.toString, call_o_sum.toString, dura_o_sum.toString, dura_o_avg.toString,
        opp_phone_sum.toString, opp_phone_std.toString, call_per_phone.toString, opp_m_phone_sum.toString,
        call_per_m_phone.toString, opp_m_phone_rate.toString, opp_f_phone_sum.toString, call_per_f_phone.toString,
        opp_f_phone_rate.toString, opp_o_phone_sum.toString, call_per_o_phone.toString, opp_o_phone_rate.toString,
        rel_b_sum.toString, rel_b_rate.toString, rel_b_m_sum.toString, rel_b_m_rate.toString, rel_r_sum.toString,
        rel_r_m_sum.toString, dura_2s.toString, dura_2s_rate.toString, dura_3to10s.toString, dura_3to10s_rate.toString,
        call_gap_less2m.toString, /*vlai_sum.toString,*/ call_per_vlai.toString, call_per_.toString, call_per_vmsc.toString,
        call_per_vmscip.toString, call_per_vcity.toString, call_per_vcountry.toString, acitve_days.toString, acitve_rate.toString,
        call_max.toString, call_min.toString, call_std.toString, worktime_sum.toString, worktime_max.toString, worktime_min.toString,
        worktime_std.toString, call_per_workhour.toString
      )


    }).toDS
    val calledFeatDs = calledDf.filter(x => x._1 != null && !x._1.isEmpty && !"null".equals(x._1) && !"NULL".equals(x._1))
      .groupByKey.map(x => {
      val phone_k = x._1
      val df = x._2
      //1.总被呼叫次数
      val called_sum = df.size
      //2.总被呼叫通话时长
      val talklens = df.map(_.talklen).map(x => nullValueProcess(x, "0").toDouble)
      val called_dura_sum = if (talklens.size == 0) {
        0.0
      } else {
        talklens.reduce(_ + _)
      }
      //3.平均被呼叫通话时长
      val called_dura_per_call = if (called_sum == 0) "null" else (called_dura_sum / called_sum).toString
      //4.被叫对端不重复号码数量
      val called_opp_phone_sum = df.map(_.rphone).toSet.size
      //5.被叫对端号码标准差
      val phone2cntMap = new mutable.HashMap[String, Int]()
      df.map(_.rphone).foreach(p => {
        phone2cntMap.+=((p, phone2cntMap.get(p).getOrElse(0) + 1))
      })
      val call_rphone_avg = called_sum / called_opp_phone_sum.toDouble
      val sumRphoneCntStandard = if (phone2cntMap.size == 0) {
        0.0
      } else {
        phone2cntMap.values.map(x => (x - call_rphone_avg) * ((x - call_rphone_avg))).reduce(_ + _)
      }
      val called_opp_phone_std = math.sqrt(sumRphoneCntStandard / called_opp_phone_sum.toDouble)
      //6.被手机呼叫次数
      val called_m_sum = df.filter(x => RegUntil.disPhoneCat(x.rphone) == "m").size
      //7.被手机呼叫率
      val called_m_rate = if (called_sum == 0) "null" else (called_m_sum / called_sum.toDouble).toString
      //8.被手机呼叫通话总时长
      val called_dura_m_s = df.filter(x => RegUntil.disPhoneCat(x.rphone) == "m").map(_.talklen)
        .map(x => nullValueProcess(x, "0"))
      val called_dura_m_sum = if (called_dura_m_s.size == 0) {
        0.0
      } else {
        called_dura_m_s.map(_.toDouble).reduce(_ + _)
      }
      //9.被固话呼叫次数
      val called_f_sum = df.filter(x => RegUntil.disPhoneCat(x.rphone) == "f").size
      //10.被固话呼叫率
      val called_f_rate = if (called_sum == 0) "null" else (called_f_sum / called_sum.toDouble).toString
      //11.被固话呼叫通话总时长
      val called_dura_f_s = df.filter(x => RegUntil.disPhoneCat(x.rphone) == "f")
        .map(_.talklen).map(x => nullValueProcess(x, "0").toDouble)

      val called_dura_f_sum = if (called_dura_f_s.size == 0) {
        0.0
      } else {
        called_dura_f_s.reduce(_ + _)
      }
      //12.被其他呼叫次数
      val called_o_sum = called_sum - called_m_sum - called_f_sum
      CsCalledFeature(
        phone_k, called_sum.toString, called_dura_sum.toString, called_dura_per_call.toString, called_opp_phone_sum.toString,
        called_opp_phone_std.toString, called_m_sum.toString, called_m_rate.toString, called_dura_m_sum.toString,
        called_f_sum.toString, called_f_rate.toString, called_dura_f_sum.toString, called_o_sum.toString
      )
    }).toDS

    val callerGroupDf = df.filter(_.subevttype == "65536")
      .select("phone", "imei", "lai", "cgi", "msc", "mscip", "vcity", "vcountry")
      .toDF("g_phone", "imei", "lai", "cgi", "msc", "mscip", "vcity", "vcountry")

    val calledGroupDf = df.filter(_.subevttype == "65538").filter(x => !x.phone.startsWith("10010"))
      .select("phone", "imei", "lai", "cgi", "msc", "mscip", "vcity", "vcountry")
      .toDF("g_phone", "imei", "lai", "cgi", "msc", "mscip", "vcity", "vcountry")

    val groupDf = callerGroupDf.union(calledGroupDf).as[CsGroupFeature]

    val groupFeatureDs = groupDf.rdd.filter(x => x.g_phone != null && !x.g_phone.isEmpty).map(x => {
      (x.g_phone, x)
    }).groupByKey.map(x => {
      val g_phone = x._1
      val iters = x._2
      //1.设备数量
      val imei_sum = iters.map(_.imei).toSet.size
      //2.常用设备
      val imei2cntMap = new mutable.HashMap[String, Int]()
      iters.map(_.imei).foreach(x => {
        imei2cntMap.+=((x, imei2cntMap.get(x).getOrElse(0) + 1))
      })
      if (imei2cntMap.contains(null)) {
        imei2cntMap.remove(null)
      }
      val com_imei = if (imei2cntMap.size == 0) {
        "-1"
      } else {
        val tmp = imei2cntMap.toArray.sortWith((a, b) => a._2 > b._2)(0)._1
        if (tmp.length >= 14) tmp.substring(0, 14) else tmp
      }
      //3.拜访lai数量
      val lais = iters.map(_.lai)
      val vlai_sum = if (lais.toArray.contains(null)) lais.toSet.size - 1 else lais.toSet.size
      //4.拜访cgi量
      val cgis = iters.map(_.cgi)
      val _sum = if (cgis.toArray.contains(null)) cgis.toSet.size - 1 else cgis.toSet.size
      //5.拜访msc数量
      val mscs = iters.map(_.msc)
      val vmsc_sum = if (mscs.toArray.contains(null)) mscs.toSet.size - 1 else mscs.toSet.size
      //6.拜访mscip数量
      val mscips = iters.map(_.mscip)
      val vmscip_sum = if (mscips.toArray.contains(null)) mscips.toSet.size - 1 else mscips.toSet.size
      //7.拜访市数量
      val vcity_sum = iters.map(_.vcity).toSet.size
      //8.拜访县数量
      val vcountry_sum = iters.map(_.vcountry).toSet.size
      //9.常在市
      val ciry2cntMap = new mutable.HashMap[String, Int]()
      x._2.map(_.vcity).filter(x => x != null && !x.isEmpty).foreach(x => {
        ciry2cntMap.+=((x, ciry2cntMap.get(x).getOrElse(0) + 1))
      })
      ciry2cntMap.remove(null)
      val com_vcity = if (ciry2cntMap.size == 0) "null" else ciry2cntMap.toArray.sortWith((a, b) => a._2 > b._2)(0)._1
      //10.常在县
      val country2cntMap = new mutable.HashMap[String, Int]()
      x._2.map(_.vcountry).filter(x => x != null && !x.isEmpty).foreach(x => {
        country2cntMap.+=((x, country2cntMap.get(x).getOrElse(0) + 1))
      })
      country2cntMap.remove(null)
      val com_vcountry = if (country2cntMap.size == 0) "null" else country2cntMap.toArray.sortWith((a, b) => a._2 > b._2)(0)._1
      //11.常在小区
      val cgi2cntMap = new mutable.HashMap[String, Int]()
      x._2.map(_.cgi).filter(x => x != null && !x.isEmpty).map(x => if (x.length >= 4) x.substring(4, x.length) else x).foreach(x => {
        cgi2cntMap.+=((x, cgi2cntMap.get(x).getOrElse(0) + 1))
      })
      cgi2cntMap.remove(null)
      val com_vcgi = if (cgi2cntMap.size == 0) "null" else cgi2cntMap.toArray.sortWith((a, b) => a._2 > b._2)(0)._1
      CsGroupFeature2(
        g_phone, imei_sum.toString, com_imei, vlai_sum.toString, _sum.toString, vmsc_sum.toString,
        vmscip_sum.toString, vcity_sum.toString, vcountry_sum.toString, com_vcity.toString, com_vcountry, com_vcgi
      )
    }).toDS

    val callerPhoneDf = getCurrentOneDayData(date, thredsholdDate).filter(x => {
      RegUntil.disPhoneCat(x.phone) == "m"
    }).filter(_.subevttype == "65536").select("phone").toDF("phone")

    val calledPhoneDf = getCurrentOneDayData(date, thredsholdDate).filter(x => {
      RegUntil.disPhoneCat(x.phone) == "m"
    }).filter(_.subevttype == "65538").select("phone").toDF("phone")
    val allPhoneDf = callerPhoneDf.union(calledPhoneDf).distinct

    //join
    val resultDf = allPhoneDf
      .join(callerFeatDs, allPhoneDf("phone") === callerFeatDs("phone_caller"), "left")
      .join(calledFeatDs, allPhoneDf("phone") === calledFeatDs("phone_called"), "left")
      .join(groupFeatureDs, allPhoneDf("phone") === groupFeatureDs("g1_phone"), "left")
      .drop("phone_caller").drop("phone_called").drop("g1_phone")
      .filter("call_sum != 0")
    val finalResultDf = resultDf.withColumn("called_per_call", resultDf("called_sum") * 1.0 / resultDf("call_sum"))
    //write hdfs
    val finalResultDfColumns = finalResultDf.columns
    print(
      s"""
         |debugInfos: ${finalResultDfColumns.mkString(",")}
      """.stripMargin)
    val savePath = "/user/dengshq/jqs/xdr/CsCallFeature." + date
    HDFSUtil.deleteFile(savePath)
    val finalResultRdd = finalResultDf.rdd.map(row => {
      (0 until (finalResultDfColumns.length)).map(i => {
        row.get(i)
      }).mkString(",")
    }).repartition(10)
    HDFSUtil.deleteFile(savePath)
    finalResultRdd.saveAsTextFile(savePath)
    spark.stop()
  }

  def  readDataSourceFromCsv(): Unit = {
    import spark.implicits._

    val reader = spark.read.option("inferSchema", "true").option("nullValue", "?").option("header", "false").csv("/user/jqs/cscall/test_0722.csv")
  }

  def getOriginDataSource(date: String, days: String): Dataset[CsCallLog] = {
    import spark.implicits._
    val startDate = DateUtil.dateAdd(date, Calendar.DAY_OF_YEAR, -days.toInt, "yyyyMMdd")
    val df = spark.sql(
      s"""
         |SELECT * FROM $ORIGIN_TAB_NAME
         |WHERE day > $startDate and day <= $date AND phone is not null AND rphone is not null
         |AND (hprovince=130 or hprovince is null)
       """.stripMargin).as[CsCallLog]
    df
  }


  def quantizeTime(time: String): Double = {
    time.substring(11, 13).toDouble + time.substring(14, 16).toDouble / 60 + time.substring(17, 19).toDouble / 3600
  }

  def getCurrentDataSource(date: String): Dataset[CsCallLog] = {
    import spark.implicits._
    val df = spark.sql(
      s"""
         |SELECT * FROM $ORIGIN_TAB_NAME
         |WHERE day = $date
         |AND phone is not null
         |AND rphone is not null
         |AND (hprovince=130 or hprovince is null)
       """.stripMargin).as[CsCallLog]
    df
  }

}
