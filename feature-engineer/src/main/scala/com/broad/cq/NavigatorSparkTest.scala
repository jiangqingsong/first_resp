package com.broad.cq
import com.boyun.common.EnvUtil._
/**
  * description: 
  *
  * @author jiangqingsong
  *         2019-10-29 12:44
  **/
object NavigatorSparkTest {

  def testNavigatorSpark(): Unit ={
    val sourcePath = ""
    spark.sparkContext.textFile(sourcePath)
  }

}
