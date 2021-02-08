package com.farben.chen

import com.farben.chen.KafkaToMysqlHbase.{getTable, reversal}
import com.sun.corba.se.impl.activation.ServerMain.{APPLICATION_ERROR, logError}
import org.apache.hadoop.hbase.client.Put
import org.apache.spark.rdd.RDD

class HbaseRunnable extends Runnable  with Serializable{

  var line:RDD[String]=_
  var family:Array[Byte]=_

  def this(line: RDD[String],family:Array[Byte])={
    this
    this.line=line
    this.family=family
  }

  override def run(): Unit = {
    line.foreachPartition(f=>{
      val table=getTable()
      try {
        f.foreach(row => {
          val strs = row.split(",")
          val rowKey=reversal(strs(0)+strs(1))
          val put = new Put(rowKey.getBytes())
          put.addColumn(family,"name".getBytes,strs(0).getBytes)
          put.addColumn(family,"age".getBytes,strs(1).getBytes)
          table.put(put)
          println("写入hbase成功")
        })
      }catch {
        case e: Exception => logError("写入HBase失败，{}" + e.getMessage)
      }

    })
  }

}
