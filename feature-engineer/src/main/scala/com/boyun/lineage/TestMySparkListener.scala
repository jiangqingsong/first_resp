package com.boyun.lineage

import org.apache.spark.sql.SparkSession
import com.boyun.common.EnvUtil._
import com.boyun.common.HDFSUtil

object TestMySparkListener {
  def sparkSourceSinkDemo(): Unit = {
    // 注册方式2：
    //spark.sparkContext.addSparkListener(new MySparkListener)
    spark.sparkContext.setLogLevel("INFO")
    //input from file.
    /**
      * source: /user/jqs/test_lineage_data.txt
      * sink: /user/jqs/lineage/result/test_wordCnt.txt
      */
    val data = spark.sparkContext.textFile("/user/jqs/test_lineage_data.txt")
    val savePath = "/user/jqs/lineage/result/test_wordCnt.txt"
    HDFSUtil.deleteFile(savePath)
    println("test savePath: " + savePath)

    //data.flatMap(_.split(" ")).map(x => (x, 1)).reduceByKey(_ + _).count()
    data.saveAsTextFile(savePath)

    //input: hive
    /*val data2 = spark.sql(
      """
        |select * from xdr.spark_c
      """.stripMargin)
    data2.count()*/

    /**
      * source: /user/jqs/kv_test.txt
      * sink: xdr.kv_test
      */
    val tab2 = "xdr.kv_test_tab"
    spark.sql(
      s"""
         |create table if not exists $tab2(
         |k string comment 'key',
         |v string comment 'value'
         |)
         |row format delimited fields terminated by ',' NULL DEFINED AS '' stored as textfile
      """.stripMargin)

    spark.sql(s"LOAD DATA INPATH '/user/jqs/kv_test.txt' INTO TABLE $tab2")

    /**
      * source: xdr.spark_c
      * sink: xdr.kv_test
      */
    spark.sql(
      s"""
         |insert overwrite table $tab2 select * from xdr.spark_c
       """.stripMargin)


    /**
      * case3: 常见外部表
      */

    /*val tab3 = "xdr.kv_test_external_tab"
    spark.sql(
      s"""
         |create external table if not exists $tab3(
         |k string comment 'key',
         |v string comment 'value'
         |)
         |row format delimited fields terminated by ',' NULL DEFINED AS '' stored as textfile
      """.stripMargin)
    spark.sql(s"LOAD DATA INPATH '/user/jqs/kv_test_1.txt' INTO TABLE $tab3")*/
    /**
      * case4: sparksql demo
      */
    val frame = spark.sql("select * from xdr.spark_c")

    /**
      * case4:
      */
    val csvDf = spark.read.csv("/user/jqs/test.csv")
    csvDf.count()
    frame.count()
    spark.stop()
  }

}
