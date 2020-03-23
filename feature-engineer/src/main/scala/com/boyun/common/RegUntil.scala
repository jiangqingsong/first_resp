package com.boyun.common

import scala.util.matching.Regex

/**
  * description: 
  *
  * @author jiangqingsong
  *         2019-10-23 14:21
  **/
object RegUntil {
  /**
    *  正则区分： 手机/固话/其他
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
}
