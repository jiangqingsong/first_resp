package com.boyun.common

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object EnvUtil {
  val spark = SparkSession.builder.appName("BoYun Spark2")
    .enableHiveSupport
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.defalut.parallelism", "1000")
    .config("spark.sql.shuffle.partitions", "1000")
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .config("spark.yarn.driver.memoryOverhead", "3072m")
    .config("spark.yarn.executor.memoryOverhead", "3072m")
    .config("spark.shuffle.file.buffer", "64k")
    .config("spark.reducer.maxSizeInFlight", "96m")
    .config("spark.shuffle.io.retryWait", "60s")
    .config("spark.rpc.askTimeout", "200s")
    .config("spark.executor.heartbeatInterval", "30s")
    .getOrCreate
// --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties"
}
