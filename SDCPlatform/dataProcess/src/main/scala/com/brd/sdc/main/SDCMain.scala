package com.brd.sdc.main

import com.brd.sdc.data_process.{AbnormalTrafficAnalysis, AggrWithVulnerability, AssetProcess, TransAndPut}

object SDCMain {
  def main(args: Array[String]): Unit = {
    println("------------------------------")
    println(args.mkString("|"))
    args(0) match {
      case "assetUpdate" => AssetProcess.assetUpdate(args(1), args(2))
      case "transAndPut" => TransAndPut.transAndPut(args(1), args(2), args(3), args(4))
      case "dpi_aggr" => AggrWithVulnerability.aggrDpiWithVulnerability(args(1), args(2), args(3), args(4))
      case "scan_aggr" => AggrWithVulnerability.aggrScanWithVulnerability(args(1), args(2), args(3), args(4))
      case "abnormal_traffic" => AbnormalTrafficAnalysis.abnormalTrafficAnalysis(args(1), args(2), args(3), args(4))
    }
  }

}
