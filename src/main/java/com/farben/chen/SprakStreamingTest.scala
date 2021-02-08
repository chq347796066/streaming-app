package com.farben.chen

import java.util

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SprakStreamingTest {

  def main(args: Array[String]){

    val conf = new SparkConf() //创建SparkConf对象
    conf.setAppName("sparkStreamingTest") //设置应用程序的名称，在程序运行的监控界面可以看到名称
    conf.setMaster("local[3]") //此时，程序在Spark集群


    /*
    * 此处设置 Batch Interval 实在spark Streaming 中生成基本Job的单位，窗口和滑动时间间隔
    * 一定是该batch Interval的整数倍*/
    val ssc = new StreamingContext(conf, Seconds(5))

    val hottestStream = ssc.socketTextStream("10.18.20.34", 9999)
    val sc=ssc.sparkContext
    val searchPair=hottestStream.map(line => {
      line.split(",")
    }).map(item=>{(item(0),item(1).toInt)})


    //reducefunction计算每个rdd的和，60s是窗口，20是滑动步长
    val hottestDStream = searchPair.reduceByKeyAndWindow((v1:Int,v2:Int) => v1 + v2, Seconds(60) ,Seconds(10))
    hottestDStream.transform(hottestItemRDD => {

      //将pair._2,pair._1反过来，通过数字来排序，然后反转，最终获取前三个打印
      val result =  hottestItemRDD.map(pair => (pair._2,pair._1) ).sortByKey(false).
        map(pair => (pair._2,pair._1))
      for(item <- result){
        println(item)
      }
      hottestItemRDD
    }).print()
    ssc.start()
    ssc.awaitTermination()
  }




}
