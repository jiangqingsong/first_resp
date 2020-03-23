package com.boyun.common

import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.collection.mutable.ArrayBuffer

/**
  * 可用用遍历hdfs上的目录
  */
object TestHdfsUtils {
  def main(args: Array[String]): Unit = {
    val path = "/aarontest/dqas"
    //生成FileSystem
    println("获取目录下的一级文件和目录")
    getFilesAndDirs(path).foreach(println)
    println("获取目录下的一级文件")
    getFiles(path).foreach(println)
    println("获取目录下的一级目录")
    getDirs(path).foreach(println)
    println("获取目录下所有文件")
    getAllFiles(path).foreach(println)
  }

  def getHdfs(path: String): FileSystem = {
    val conf = new Configuration()
    FileSystem.newInstance(URI.create(path), conf)
  }

  //获取目录下的一级文件和目录
  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  //获取目录下的一级文件
  def getFiles(path: String): Array[String] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
      .map(_.toString)
  }

  //获取目录下的一级目录
  def getDirs(path: String): Array[String] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory)
      .map(_.toString)
  }

  //获取目录下的所有文件
  def getAllFiles(path: String): ArrayBuffer[String] = {
    val arr = ArrayBuffer[String]()
    val hdfs = getHdfs(path)
    val getPath = getFilesAndDirs(path)
    getPath.foreach(patha => {
      if (hdfs.getFileStatus(patha).isFile())
        arr += patha.toString
      else {
        arr ++= getAllFiles(patha.toString())
      }
    })
    arr
  }

}
