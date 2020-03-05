package com.brd.sdc.main

import com.brd.sdc.data_process.{AssetProcess, TransAndPut}

object SDCMain {
  def main(args: Array[String]): Unit = {
    args(0) match {
      case "assetUpdate" => AssetProcess.assetUpdate(args(1), args(2))
      case "transAndPut" => TransAndPut.transAndPut(args(1), args(2), args(3))
    }
  }

}
