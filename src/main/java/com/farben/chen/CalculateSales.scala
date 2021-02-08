package com.farben.chen

import com.farben.chen.KafkaToMysqlHbase.{saveToHbase, saveToMysql}
import com.farben.chen.util.CalculateUtil
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object CalculateSales {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ScalaKafkaStream")
      .setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    val bootstrapServers = "hadoop103:9092,hadoop101:9092,hadoop105:9092"
    val groupId = "ocean"
    val topicName = "farben-sales"
    val maxPoll = 20000

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))
    kafkaTopicDS.map(_.value).window(Seconds(60),Seconds(10))
      .foreachRDD(line=> {
        if(line!=null&&line!="")
          CalculateUtil.calculateTotalSales(line)
    })
    ssc.start()
    ssc.awaitTermination()
  }

}
