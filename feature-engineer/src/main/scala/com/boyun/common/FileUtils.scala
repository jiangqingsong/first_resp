package com.boyun.common

import java.util

import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import scala.collection.mutable.ArrayBuffer

import scala.collection.mutable.ArrayBuffer
object FileUtils {
  def traverseDir(path: String): util.ArrayList[String] ={
    val pathList = new util.ArrayList[String]()
    val files = FileSystem.get(new Configuration).listStatus(new Path(path))
    files.map(x => {
      if(x.isDirectory){
        traverseDir(x.getPath.getName)
      }else{
        pathList.add(x.getPath.getName)
      }
    })
    pathList
  }

  def getHdfs(path: String) = {
    val conf = new Configuration()
    FileSystem.get(URI.create(path), conf)
  }
  def getFilesAndDirs(path: String): Array[Path] = {
    val fs = getHdfs(path).listStatus(new Path(path))
    FileUtil.stat2Paths(fs)
  }

  /**************直接打印************/

  /**
    * 打印所有的文件名，包括子目录
    */
  def listAllFiles(path: String) {
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile())
        println(path)
      else {
        listAllFiles(path.toString())

      }
    })
  }
  /**
    * 打印一级文件名
    */
  def listFiles(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile()).foreach(println)
  }

  /**
    * 打印一级目录名
    */
  def listDirs(path: String) {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory()).foreach(println)
  }
  /**
    * 打印一级文件名和目录名
    */
  def listFilesAndDirs(path: String) {
    getFilesAndDirs(path).foreach(println)
  }

  /**************直接打印************/

  /**************返回数组************/
  def getAllFiles(path: String): ArrayBuffer[Path] = {
    val arr = ArrayBuffer[Path]()
    val hdfs = getHdfs(path)
    val listPath = getFilesAndDirs(path)
    listPath.foreach(path => {
      if (hdfs.getFileStatus(path).isFile()) {
        arr += path
      } else {
        arr ++= getAllFiles(path.toString())
      }

    })
    arr
  }
  def getFiles(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isFile())
  }
  def getDirs(path: String): Array[Path] = {
    getFilesAndDirs(path).filter(getHdfs(path).getFileStatus(_).isDirectory())
  }

}
