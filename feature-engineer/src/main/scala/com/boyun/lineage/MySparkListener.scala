package com.boyun.lineage

import org.apache.spark.scheduler._

class MySparkListener extends SparkListener {


  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    //println(taskStart.stageId)
  }


  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = {
    //println(taskGettingResult.taskInfo)
  }


  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    //println(blockManagerAdded.time)
  }


  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    blockManagerRemoved.time
  }


  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    println(applicationStart.time)
    println("appId: " + applicationStart.appId)
    println("appName: " + applicationStart.appName)
    println("sparkUser: " + applicationStart.sparkUser)
    println("appAttemptId: " + applicationStart.appAttemptId)
  }


  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    applicationEnd.time
  }


  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println(taskEnd.stageId)
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.stageInfos.map(x => {
      x.rddInfos.map(y => {
        println("rddInfo.name: " + y.name)
        println("rddInfo.parentIds: " + y.parentIds.seq.mkString(","))
      })
    })

  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    //println(111)
  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    //println(111)
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = {
    //println(111)
  }

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    //println(111)
  }

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    //println(111)
  }

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    //println(111)
  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    //println(111)
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    //println(111)
  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = {
    println(stageSubmitted.stageInfo.taskMetrics)
    println(stageSubmitted.stageInfo.rddInfos.seq.mkString("|"))
    println(stageSubmitted.stageInfo.stageId)
    println(stageSubmitted.stageInfo)
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    println(stageCompleted.stageInfo.rddInfos)

  }

  def printx(label: String): Unit = {
    println(s"=" * 20 + label + s"=" * 20)
  }

}
