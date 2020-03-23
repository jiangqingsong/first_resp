package com.broad.cq.HighSpeedRailWay

import com.boyun.common.EnvUtil._
import com.boyun.common.DateUtil._
import com.boyun.common.HDFSUtil
import com.broad.cq.HighSpeedRailWay.HighSpeedRailWayCompute.getMaxMinTime

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * 江苏：识别高铁用户
  */
object HighSpeedRailWayCompute {
  val partten = "yyyy-MM-dd hh:mm:ss"

  import spark.implicits._


  def loadTimetable(date: String): Map[(String, String), Array[(String, String, String)]] = {
    val df = spark.sql(
      s"""
         |select
         |direction,
         |traincodel,
         |traincodes,
         |stationnum,
         |stationname,
         |arrive_time,
         |leave_time,
         |stay_duration,
         |day from soc_config.cfg_train_stations
         |where day='20191123' and
         |stationname in ('徐州东', '南京南', '无锡东', '常州北', '镇江南', '丹阳北', '苏州北', '昆山南')
       """.stripMargin)
    df.as[TimeTable].map(x => {
      ((x.stationname, x.direction), (x.arrive_time, x.leave_time, x.traincodes))
    })
      .rdd.groupByKey().map(x => (x._1, x._2.toArray)).collect().toMap
  }

  def loadUserSample(date: String): Dataset[UserSample] = {
    val df = spark.sql(
      s"""
         |select
         |mid1.msisdn,
         |mid1.starttime,
         |mid1.endtime,
         |mid1.lon,
         |mid1.lat,
         |mid1.datasrc,
         |mid1.stationname,
         |mid1.stationnum
         |from
         |(select msisdn from soc_fishing.h_suspectedusr where day='$date') mid0
         |join
         |(select s.* from soc_fishing.h_railway_sample s where day='$date') mid1
         |on mid0.msisdn = mid1.msisdn
      """.stripMargin).as[UserSample]
    df
  }

  def chrUsrIdentify(date: String): Unit = {
    val sc = spark.sparkContext
    val stations = new mutable.HashMap[String, String]()
    stations.+=(("徐州东站", "徐州东站"))
    stations.+=(("山东江苏", "山东(北京)境内"))
    stations.+=(("江苏安徽", "安徽境内"))
    stations.+=(("安徽江苏", "安徽江苏"))
    stations.+=(("南京南站", "南京南站"))
    stations.+=(("镇江南站", "镇江南站"))
    stations.+=(("丹阳北站", "丹阳北站"))
    stations.+=(("常州北站", "常州北站"))
    stations.+=(("无锡东站", "无锡东站"))
    stations.+=(("苏州北站", "苏州北站"))
    stations.+=(("昆山南站", "昆山南站"))
    stations.+=(("江苏上海", "上海(虹桥)"))
    //read usersample
    //val userSampleDs = readUserSample(date) //ok
    val userSampleDs = readUserSample(s"/user/jqs/highSpeedRailWay/suspectedusr.$date.csv") //ok
    userSampleDs.cache()
    //read distanceConfig
    val distanceMap = readDistanceConfig("/user/jqs/highSpeedRailWay/distance_config.csv")
    val distanceMapBC = sc.broadcast(distanceMap)
    //计算用户区间最大速度

    val user2maxSpeed = userSampleDs.filter(x => !x.stationname.isEmpty).map(x => (x.msisdn, x)).rdd.groupByKey().map(x => {
      //(x._1, x._2.size)
      val distanceMap = distanceMapBC.value
      val user = x._1
      val sortedUserSamples = x._2.toArray.sortWith((u1, u2) => u1.starttime < u2.starttime)
      val indexSortUserSamples = sortedUserSamples.zipWithIndex
      val left = indexSortUserSamples.slice(0, sortedUserSamples.length - 1)
      //0\1\2
      val right = indexSortUserSamples.tail
      var maxSpeed = 0.0
      if (sortedUserSamples.length > 0) {
        maxSpeed = if (right.size == 0) 0 else {
          right.map(x => {
            val index = x._2 - 1 // right的index会比left对应的大1
            val rightStartTime = x._1.starttime.substring(0, 19)
            val leftStartTime = left(index)._1.starttime.substring(0, 19)
            val gap = getGapSeconds(partten, rightStartTime, leftStartTime)
            val distance = if (gap >= 300 && gap <= 3600) {
              val rightStationnum = x._1.stationnum.toLong
              val leftStationnum = left(index)._1.stationnum.toLong
              distanceMap.get(rightStationnum - 1).get(leftStationnum.toInt - 1)
            } else {
              0
            }
            val speed = distance / gap * 60 * 60
            if (speed <= 380) speed else 0
          }).max
        }
      }
      (user, maxSpeed)
    })
    user2maxSpeed.cache()
    //end  step1 ok.
    /*val user2maxSpeedPath = "/user/jqs/highSpeedRailWay/user2maxSpeed." + date
    saveStringRdd(user2maxSpeedPath, user2maxSpeed.map(x => x._1 + "," + x._2).repartition(2))*/

    val highSpeedUserMap = user2maxSpeed.filter(x => x._2 >= 250).map(x => (x._1, 1)).collect().toMap
    val highSpeedBC = sc.broadcast(highSpeedUserMap)

    val highSpeedUserSample = userSampleDs.filter(x => {
      val highSpeedMap = highSpeedBC.value
      highSpeedMap.getOrElse(x.msisdn, 0) == 1 && !x.stationname.isEmpty
    })

    val speed_250_results = highSpeedUserSample.rdd.map(x => (x.msisdn, x)).groupByKey().map(x => {
      val user = x._1
      //记录用户每个station的进站记录index
      val taglst = new ArrayBuffer[Int]()
      taglst.append(0)
      val withIndexSamples = x._2.toSeq.sortWith(_.starttime < _.starttime).zipWithIndex.toArray
      for (index <- (1 until withIndexSamples.size)) {
        if (!withIndexSamples(index)._1.stationname.substring(0, 4).equals(withIndexSamples(index - 1)._1.stationname.substring(0, 4))) {
          taglst.append(index)
        }
      }
      val str_arr_time = new ArrayBuffer[(String, String, String, String, String, String, String, String)]()

      for (index <- (0 until taglst.size)) {
        var period_s = ""
        var period_e = ""
        var direction = ""
        var kind = ""
        var station_s = ""
        var station_e = ""
        val currentUserSample = withIndexSamples(taglst(index))
        val station = currentUserSample._1.stationname.substring(0, 4)
        val msisdn = currentUserSample._1.msisdn

        var tmp = Array[(UserSample, Int)]()
        //end station
        if (index == taglst.size - 1) {
          tmp = withIndexSamples.slice(taglst(index), withIndexSamples.length)
          period_s = withIndexSamples(taglst(index))._1.starttime
          period_e = withIndexSamples.last._1.starttime
          direction = if (currentUserSample._1.stationnum.toInt > withIndexSamples(taglst(index - 1))._1.stationnum.toInt) "P" else "N"
          kind = "end_station"
        } else {
          tmp = withIndexSamples.slice(taglst(index), taglst(index + 1))
          period_s = currentUserSample._1.starttime
          period_e = withIndexSamples(taglst(index + 1) - 1)._1.starttime
          if (index == 0) {
            direction = if (withIndexSamples(taglst(index))._1.stationnum.toInt < withIndexSamples(taglst(index + 1))._1.stationnum.toInt) "P" else "N"
            kind = "start_station"
          } else if (getGapMinutes(partten, period_s.substring(0, 19), period_e.substring(0, 19)) >= 60) {
            val direction1 = if (withIndexSamples(taglst(index))._1.stationnum.toInt < withIndexSamples(taglst(index + 1))._1.stationnum.toInt) "P" else "N"
            val direction2 = if (withIndexSamples(taglst(index - 1))._1.stationnum.toInt < withIndexSamples(taglst(index))._1.stationnum.toInt) "P" else "N"
            direction = direction1 + direction2
            kind = "transfer_station"
          } else {
            val direction1 = if (withIndexSamples(taglst(index))._1.stationnum.toInt < withIndexSamples(taglst(index + 1))._1.stationnum.toInt) "P" else "N"
            val direction2 = if (withIndexSamples(taglst(index - 1))._1.stationnum.toInt < withIndexSamples(taglst(index))._1.stationnum.toInt) "P" else "N"
            direction = direction1 + direction2
            kind = "middle_station"
          }
        }
        val filteredTmp = tmp.filter(x => x._1.stationname.length == 4)
        if (filteredTmp.length == 0 || getGapMinutes(partten, period_s.substring(0, 19), period_e.substring(0, 19)) <= 5) {
          station_s = ""
          station_e = ""
        } else if (getGapMinutes(partten, period_s.substring(0, 19), period_e.substring(0, 19)) > 5) {
          station_s = tmp.filter(x => x._1.stationname.length == 4).head._1.starttime
          station_e = tmp.filter(x => x._1.stationname.length == 4).last._1.starttime
        }
        str_arr_time.append((msisdn, station, period_s, period_e, station_s, station_e, direction, kind))
      }
      (user, str_arr_time)
    }).flatMap(x => {
      x._2.map(a => a)
    }).filter(x => {
      x._2.equals("南京南站") || !x._5.isEmpty || !x._8.equals("middle_station") || !x._8.equals("transfer_station")
    }) //msisdn, station, period_s, period_e, station_s, station_e, direction, kind

    //read config
    //val timeTableMap = loadTimetable(date)
    val timeTableMap = sc.textFile("/user/jqs/highSpeedRailWay/Chr_timetable.csv").map(_.split("\t")).filter(_.length == 9)
      .map(x => TimeTable(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8)))
      .map(x => {
        ((x.stationname, x.direction), (x.arrive_time, x.leave_time, x.traincodes))
      }).groupByKey().map(x => {
      (x._1, x._2.toArray)
    }).collect().toMap
    val timeTableBC = sc.broadcast(timeTableMap)
    val df_file_speed_250 = speed_250_results.map(x => {
      //msisdn, station, period_s, period_e, station_s, station_e, direction, kind
      val timeTableMap = timeTableBC.value
      val msisdn = x._1
      val station = x._2
      val period_s = x._3
      val period_e = x._4
      val station_s = x._5
      val station_e = x._6
      val direction = x._7
      val kind = x._8
      val match_res = new ArrayBuffer[String]()
      if (station.substring(3, 4).equals("站")) {
        if (station_s.isEmpty || period_s.isEmpty) {
          match_res.append("")
        } else if (period_s.equals(station_s) && !period_e.equals(station_e)) {
          if ("transfer_station".equals(kind)) {
            if (getGapMinutes(partten, period_e, period_s) >= 20) {
              match_res.append("")
              match_res.append("")
            } else {
              match_res.append("")
              match_res.append(matching_algorithms(timeTableMap, station, direction.substring(0, 1), period_s, period_e, "leave"))
            }
          } else if (!"end_station".equals(kind)) {
            match_res.append(matching_algorithms(timeTableMap, station, direction.substring(0, 1), period_s, period_e, "leave"))
          } else {
            match_res.append("")
          }
        } else if (!period_s.equals(station_s) && period_e.equals(station_e)) {
          if ("transfer_station".equals(kind)) {
            if (getGapMinutes(partten, period_e, period_s) >= 20) {
              match_res.append("")
              match_res.append("")
            } else {
              match_res.append(matching_algorithms(timeTableMap, station, direction.substring(0, 1), period_s, period_e, "arrive"))
              match_res.append("")
            }
          } else if (!"start_station".equals(kind)) {
            match_res.append(matching_algorithms(timeTableMap, station, direction.substring(0, 1), period_s, period_e, "arrive"))
          } else {
            match_res.append("")
          }
        } else if (!period_s.equals(station_s) && !period_e.equals(station_e)) {
          if ("start_station".equals(kind)) {
            match_res.append(matching_algorithms(timeTableMap, station, direction.substring(0, 1), station_s, period_e, "leave"))
          } else if ("end_station".equals(kind)) {
            match_res.append(matching_algorithms(timeTableMap, station, direction.substring(0, 1), period_s, station_e, "arrive"))
          } else if ("middle_station".equals(kind)) {
            val a = matching_algorithms(timeTableMap, station, direction.substring(0, 1), period_s, station_e, "arrive")
            val b = matching_algorithms(timeTableMap, station, direction.substring(0, 1), station_s, period_e, "leave")
            match_res.append(a)
            match_res.append(b)
          } else if ("transfer_station".equals(kind)) {
            val a = if (getGapMinutes(partten, period_s, station_s) >= 20) {
              ""
            } else {
              matching_algorithms(timeTableMap, station, direction.substring(0, 1), period_s, station_s, "arrive")
            }
            val b = if (getGapMinutes(partten, station_e, period_e) >= 20) {
              ""
            } else {
              matching_algorithms(timeTableMap, station, direction.last.toString, station_e, period_e, "leave")
            }
            match_res.append(a)
            match_res.append(b)
          }
          else {
            match_res.append("")
          }
        } else {
          match_res.append("")
        }
      } else {
        match_res.append("")
      }
      val new_direction = direction.distinct
      val dir_label = new_direction.size
      (msisdn, station, period_s, period_e, station_s, station_e, new_direction, kind, match_res, dir_label)
    })

    df_file_speed_250.cache()
    val df_tmp0 = df_file_speed_250.filter(x => x._10 == 2)
    val df_tmp1 = df_file_speed_250.filter(x => x._10 == 2)
    val df_tmp2 = df_file_speed_250.filter(x => x._10 == 1)

    val df_tmp3 = df_tmp0.map(x => {
      (x._1, x._2, x._3, x._4, x._5, x._6, x._7(0).toString, x._8, if (x._9.size == 2) x._9.slice(0, 1) else x._9, x._10)
    })
    val df_tmp4 = df_tmp1.map(x => {
      (x._1, x._2, x._3, x._4, x._5, x._6, x._7(1).toString, x._8, if (x._9.size == 2) x._9.slice(1, 2) else x._9, x._10)
    })
    val df_new = df_tmp2.union(df_tmp3).union(df_tmp4)


    val finalRdd = df_new.map(x => {
      //((msisdn, direction),(...))
      ((x._1, x._7), x)
    }).groupByKey().map(x => {
      //x: msisdn, station, period_s, period_e, station_s, station_e, new_direction, kind, match_res, dir_label
      val sorted = x._2.toArray.sortWith((a, b) => a._3 < b._3)
      val start = stations.getOrElse(sorted.head._2, "")
      //val phone = sorted.head._1
      val end = stations.getOrElse(sorted.last._2, "")
      val start_time = sorted.head._4
      val end_time = sorted.last._3
      val cs2cntMap = new mutable.HashMap[String, Int]()

      var tag = ""
      sorted.map(_._9).foreach(x => {
        if (x.length == 1 && !x(0).equals("")) {
          tag = tag + "&" + x(0)
        } else if (x.length == 2) {
          val filtered = x.filter(x => !x.equals(""))
          if (filtered.length == 1) {
            tag = tag + "&" + filtered(0)
          } else if (filtered.length == 2) {
            tag = tag + "&" + filtered.mkString("&")
          }
        }
      })
      tag.split("&").filter(x => !x.equals("")).foreach(res => {
        if (!cs2cntMap.contains(res)) cs2cntMap.+=((res, 1)) else cs2cntMap.+=((res, cs2cntMap.get(res).get + 1))
      })
      var key_tag = ""
      var possibility = 0.0

      if (cs2cntMap.size != 0) {
        val maxTup = cs2cntMap.toSeq.sortWith(_._2 > _._2).head
        possibility = maxTup._2 / 1.0 / cs2cntMap.values.sum
        key_tag = maxTup._1
      }
      //(x._1._1, start, end, start_time, end_time, key_tag, if(possibility == 0.0) "" else possibility.toString, sorted.map(_._9).mkString("|"))
      x._1._1 + "\t" + start + "\t" + end + "\t" + start_time + "\t" + end_time + "\t" + key_tag + "\t" + (if (possibility == 0.0) "" else possibility.toString)
    })

    saveStringRdd("/user/jqs/highSpeedRailWay/final_info." + date, finalRdd.repartition(10))
  }

  def saveStringRdd(path: String, rdd: RDD[String]): Unit = {
    HDFSUtil.deleteFile(path)
    rdd.saveAsTextFile(path)
  }

  def matching_algorithms(timeTableMap: Map[(String, String), Array[(String, String, String)]], station: String, direction: String,
                          t1: String, t2: String, label: String): String = {
    val default = Array(("00:00", "00:00", "null"))
    val res_codes = timeTableMap.getOrElse((station.substring(0, 3), direction), default).filter(x => {
      val arrive_time = x._1
      val leave_time = x._2
      val timeTup = getMaxMinTime(t1.substring(11, 16), t2.substring(11, 16))
      if (label.equals("arrive")) {
        arrive_time >= timeTup._2 && arrive_time <= timeTup._1
      } else if (label.equals("leave")) {
        leave_time >= timeTup._2 && leave_time <= timeTup._1
      } else {
        false
      }
    }).map(_._3)
    if (res_codes.size == 0) "" else res_codes.mkString("&")
  }

  def getMaxMinTime(t1: String, t2: String): (String, String) = {
    val max = if (t1 > t2) t1 else t2
    val min = if (t1 < t2) t1 else t2
    (max, min)
  }

  /**
    *
    * @param date
    * @return
    */
  def readUserSample(/*date: String*/ userSamplePath: String): Dataset[UserSample] = {
    import spark.implicits._
    val userSample = spark.sparkContext.textFile(userSamplePath).map(_.split("\t"))
      .filter(_.length == 8).map(x => UserSample(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7))).toDS()
    //val userSample = loadUserSample(date)
    userSample.cache

    //按部分col为空drop
    val fileteredUserSampleDs = userSample.filter(row => {
      //val subArr = Array("msisdn", "starttime", "endtime", "lon", "lat", "datasrc")
      !(row.msisdn.isEmpty || row.starttime.isEmpty || row.endtime.isEmpty || row.lon.isEmpty || row.lat.isEmpty || row.datasrc.isEmpty)
    })
    //按部分col去重
    val finalUserSampleDs = fileteredUserSampleDs.dropDuplicates(Array("msisdn", "starttime", "endtime", "lon", "lat", "stationname", "stationnum"))
    finalUserSampleDs
  }

  /**
    * 读取距离配置文件
    *
    * @param path
    */
  def readDistanceConfig(path: String): Map[Long, Array[Double]] = {
    val distanceMap = spark.sparkContext.textFile(path).zipWithIndex().map(x => {
      val distances = x._1.split(",").map(_.toDouble)
      val index = x._2
      (index, distances)
    }).collect().toMap
    distanceMap
  }

}


case class UserSample(
                       msisdn: String,
                       starttime: String,
                       endtime: String,
                       lon: String,
                       lat: String,
                       datasrc: String,
                       stationname: String,
                       stationnum: String
                     )

case class TimeTable(
                      direction: String,
                      traincodel: String,
                      traincodes: String,
                      stationnum: String,
                      stationname: String,
                      arrive_time: String,
                      leave_time: String,
                      stay_duration: String,
                      day: String
                    )

case class OutClass(
                     msisdn: String,
                     start: String,
                     end: String,
                     start_time: String,
                     end_time: String,
                     key_tag: String,
                     possibility: String
                   )