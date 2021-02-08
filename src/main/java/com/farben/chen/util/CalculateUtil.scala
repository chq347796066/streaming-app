package com.farben.chen.util

import org.apache.spark.rdd.RDD

object CalculateUtil {

  def calculateTotalSales(str: RDD[String]) = {
    //将计算结果写入redis
    val totalRdd = str.map(line => {
      //string 类型,要进行转换
      val arr=line.split(",")
      val sales = arr(1).toLong
      sales
    })
    if(!totalRdd.isEmpty()){
      val sum=totalRdd.reduce(_+_)
      println("sum=="+sum)
      val conn = JedisConnectionPool.getConnection()
      conn.incrByFloat("totalSales",sum)
      val totalSales = conn.get("totalSales")
      println("totalSales:"+totalSales)
      conn.close()
    }
  }

}
