package com.brd.sdc.common
import com.brd.sdc.common.SparkEnvUtil._

import scala.collection.mutable
import com.brd.sdc.common.DateUtil._
/**
 * @author jiangqingsong
 * @description
 * @date 2020-03-17 14:08
 */
object TestDemo {
  def main(args: Array[String]): Unit = {
    val date = "20200323"
    println(getPreMulHoursTime(-168, "yyyyMMdd", date))
  }
}
