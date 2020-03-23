package com.boyun.parse_yarn_log

import java.util.Properties
import java.io.FileInputStream

import com.boyun.common.EnvUtil._
import com.boyun.common.DateUtil

/**
  * 解析yarn日志
  */
object ParseYarnLog {
  val SOURCE_KEY = "sourceKey"
  val SINK_KEY = "sinkKey"
  val HIVE_KEY = "hiveKey"
  val URL = "url"
  val DBTABLE = "dbtable"
  val USER = "user"
  val PASSWORD = "password"
  val PATTERN = "yyyy-MM-dd HH:mm:ss"

  /**
    *
    * @param properPath properties path (eg: application.properties)
    * @param startTime  开始处理的时间 (eg: 2019-11-12 08:00:00)
    * @param gapHours 处理时间的窗口大小 (eg: 1)
    */
  def parseYarnLog(properPath: String, startTime: String, gapHours: String): Unit = {
    import spark.implicits._
    //load properties.
    val props = new Properties()
    props.load(new FileInputStream(properPath))
    val sourceKey = props.getProperty(SOURCE_KEY)
    val sinkKey = props.getProperty(SINK_KEY)
    val hiveKey = props.getProperty(HIVE_KEY)
    //mysql
    val url = props.getProperty(URL)
    val username = props.getProperty(USER)
    val password = props.getProperty(PASSWORD)
    val dbtable = props.getProperty(DBTABLE)

    val endTime = DateUtil.getPreMulHoursTime(gapHours.toInt, PATTERN, startTime)

    val dataDF = spark.read.format("jdbc")
      .option("url", s"${url}?useUnicode=true&characterEncoding=UTF-8&serverTimezone=UTC")
      .option("dbtable", dbtable)
      .option("user", username)
      .option("password", password)
      .load()
    dataDF.createOrReplaceTempView("tmptable")

    val mysqlDf = spark.sql(s"select * from tmptable where formattedFinishedTime>='$startTime' and formattedFinishedTime<'$endTime'")

    mysqlDf.as[SparkApplication].rdd.map(x => {
      (x.app_id, x.user)
    }).foreachPartition(i => {
      i.foreach(tup => {

        val appId = tup._1
        val logPath = "/tmp/logs/" + tup._2 + "/logs/" + appId
        val infoLevelLogRdd = spark.sparkContext.textFile(logPath).filter(x => x.contains("INFO"))

        infoLevelLogRdd.cache()

        val sourceLog = infoLevelLogRdd.filter(x => x.contains(sourceKey))
        val sinkLog = infoLevelLogRdd.filter(x => x.contains(sinkKey))

        //source parse
        val sourceListRdd = sourceLog.map(_.split(sourceKey)).filter(_.length == 2).map(x => x(1)).map(x => {
          if (x.contains(hiveKey)) {
            val eSplits = x.split(hiveKey)(1).split("/")
            val dcName = eSplits(0).replace(".db", "")
            val tableName = eSplits(1)
            dcName + "." + tableName
          } else {
            val tailInfo = x.split(":").last
            val fileName = x.replace(tailInfo, "")
            fileName
          }
        })
        //sink parse
        val sinklistRdd = sinkLog.map(_.split(sinkKey)).filter(_.length == 2).map(x => x(1)).map(_.split(" to "))
          .filter(_.length == 2).map(x => x(1)).map(x => {
          if (x.contains(hiveKey)) {
            val eSplits = x.split(hiveKey)(1).split("/")
            val dcName = eSplits(0).replace(".db", "")
            val tableName = eSplits(1)
            dcName + "." + tableName
          } else {
            if (x.contains("/_temporary")) {
              x.split("/_temporary")(0)
            } else {
              x
            }
          }
        })

        val sourceSet = sourceListRdd.collect.distinct
        val sinkSet = sinklistRdd.collect.distinct


        //todo  sink to kafka.

        infoLevelLogRdd.unpersist()
      })
    })

  }
}

case class SparkApplication(
                             app_id: String,
                             user: String,
                             name: String,
                             state: String,
                             finalStatus: String,
                             trackingUI: String,
                             trackingUrl: String,
                             clusterId: String,
                             applicationType: String,
                             applicationTags: String,
                             startedTime: String,
                             finishedTime: String,
                             elapsedTime: String,
                             logAggregationStatus: String,
                             formattedFinishedTime: String,
                             sources: String,
                             sinks: String
                           )
