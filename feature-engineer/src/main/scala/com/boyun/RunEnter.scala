package com.boyun

import com.boyun.data._
import com.boyun.lineage.TestMySparkListener
import com.boyun.parse_yarn_log.ParseYarnLog
import com.broad.cq.HighSpeedRailWay.HighSpeedRailWayCompute

object RunEnter {
  def main(args: Array[String]): Unit = {
    args(0) match {
      case "roaming_feature"   => RoamingFeature.dataProcee(args(1), args(2), args(3))
      case "testSourceData"    => RoamingFeature.testSourceData(args(1), args(2))
      case "case_analysis"     => DataAnalysis.caseAnalysisi(args(1), args(2), args(3))
      case "cs_call_feature"   => CsCalllogAcess.extractFeatures(args(1), args(2), args(3), args(4))
        //lineage test
      case "testSparkListener" => TestMySparkListener.sparkSourceSinkDemo()
      case "parseYarnLog"      => ParseYarnLog.parseYarnLog(args(1), args(2), args(3))
        //highSpeedRailWay
      case "chrUsrIdentify"      => HighSpeedRailWayCompute.chrUsrIdentify(args(1))
    }
  }
}
