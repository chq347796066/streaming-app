package com.farben.chen


import java.sql.{Connection, DriverManager, PreparedStatement}

import com.sun.corba.se.impl.activation.ServerMain.logError
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Table}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object KafkaToMysqlHbase {

  def saveToHbase(line: RDD[String],family:Array[Byte]): Unit = {
    line.foreachPartition(f=>{
      val table=getTable()
      try {
        f.foreach(row => {
          val strs = row.split(",")
          val rowKey=reversal(strs(0))
          val put = new Put(rowKey.getBytes())
          put.addColumn(family,"name".getBytes,strs(0).getBytes)
          put.addColumn(family,"age".getBytes,strs(1).getBytes)
          table.put(put)
          println(strs(0)+"写入hbase成功")
        })
      }catch {
        case e: Exception => logError("写入HBase失败，{}" + e.getMessage)
      }

    })
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("ScalaKafkaStream")
      //.setMaster("local[2]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(5))

    val bootstrapServers = "hadoop103:9092,hadoop101:9092,hadoop105:9092"
    val groupId = "ocean"
    val topicName = "farben-streaming"
    val maxPoll = 20000

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )
    //读kafka数据
    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))
    val family="cf1".getBytes()
    kafkaTopicDS.map(_.value)
      .map(x=>{
        x
      }).foreachRDD(line=> {
      //落盘
      saveToMysql(line)
      saveToHbase(line,family)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def saveToMysql(line:RDD[String])={
    var conn: Connection = null;
    var ps: PreparedStatement = null;
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      line.foreachPartition(f => {
        conn = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/bdp?useUnicode=true&characterEncoding=utf8", "root",
          "Farben2019@");
        ps = conn.prepareStatement("insert into person (name,age) values(?,?)");
        f.foreach(s => {
          val strs = s.split(",")
          ps.setString(1, strs(0));
          ps.setInt(2, strs(1).toInt);
          ps.executeUpdate();
          println(strs(0)+"写入mysql成功")
        })
      })
    } catch {
      case t: Throwable => t.printStackTrace() // TODO: handle error
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close();
      }
    }
  }

  def getTable (): Table = {
    val hbaseConf = HBaseConfiguration.create ()
    hbaseConf.set ("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104")
    hbaseConf.set ("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set ("hbase.defaults.for.version.skip", "true")
    val conn = ConnectionFactory.createConnection (hbaseConf)
    conn.getTable (TableName.valueOf ("person") )
  }


  def reversal(str:String)={
    val sb=new StringBuilder(str)
    sb.reverse.toString()
  }



}
