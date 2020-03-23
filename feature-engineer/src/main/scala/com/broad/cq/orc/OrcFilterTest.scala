package com.broad.cq.orc

import com.boyun.common.EnvUtil._
import com.boyun.common.DateUtil._
import com.boyun.common.HDFSUtil
object OrcFilterTest {
  def orcFilter(): Unit ={
    val df = spark.read.orc("")
    df.filter("")
  }

}
