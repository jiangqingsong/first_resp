package com.boyun.lineage

import org.apache.spark.sql.SparkSession

object LocalTestListener {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("MySparkListener Test.")
      .master("local[2]")
      .config("spark.extraListeners","com.boyun.lineage.MySparkListener")//注册方式1：
      .getOrCreate()
    val df = spark.sql("select * from xdr.spark_c")
    df.count()
    val rdd = spark.sparkContext.textFile("file:///E:/linage_data_test/test_data.txt")
    rdd.saveAsTextFile("file:///E:/linage_data_test/test.out")
    println(rdd.count())
  }

}
