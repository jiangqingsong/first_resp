package com.boyun.common
import com.boyun.common.EnvUtil._
import com.boyun.data.CsCalllogAcess
import org.apache.spark.rdd.RDD

import scala.util.Random

object DataProcessUtil {


  def processIncline(pairRdd: RDD[(String, Long)], randomNum: Int, randomKeys: String, separator: String = ",", randomSeparator: String): Unit ={
    //val key2cntRdd = pairRdd.map(x =>(x._1, 1)).reduceByKey(_ + _).sortBy(_._2, false)
    //找到需要做随机打散的key
    //val sum = key2cntRdd.map(_._2).reduce(_ + _)
    val randomKeyArr = randomKeys.split(separator)
    val needRandomPairRdd = pairRdd.filter(x => randomKeyArr.contains(x._1))
    val notNeedRandomPairRdd = pairRdd.filter(x => !randomKeyArr.contains(x._1))

    needRandomPairRdd.map(x => {
      val randomPrefix = new Random().nextInt(randomNum)
      (randomPrefix + randomSeparator + x._1, x._2)
    })
    //TODO: 可以传入function来做聚合处理
    needRandomPairRdd
    val part1Rdd = aggr(needRandomPairRdd)
    val part2Rdd = aggr(notNeedRandomPairRdd)

  }

  def aggr(pairRdd: RDD[(String, Long)]):  RDD[(String, Long)] ={
    pairRdd.groupByKey.map(x => (x._1, x._2.size))
  }
  def testAggr(): Unit ={
    val originDs = CsCalllogAcess.getOriginDataSource("20190901", "15")
    originDs.rdd.map(x => (x.phone, x))
  }
}
