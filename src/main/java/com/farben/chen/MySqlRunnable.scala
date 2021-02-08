package com.farben.chen

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.rdd.RDD

class MySqlRunnable extends Runnable with Serializable {
  var line:RDD[String]=_

  def this(line:RDD[String])={
    this
    this.line=line
  }
  override def run(): Unit = {
    var conn: Connection = null;
    var ps: PreparedStatement = null;
    try {
      Class.forName("com.mysql.jdbc.Driver").newInstance();
      line.foreachPartition(f => {
        conn = DriverManager.getConnection("jdbc:mysql://10.18.20.34:3306/bdp?useUnicode=true&characterEncoding=utf8", "root",
          "Farben2019@");
        ps = conn.prepareStatement("insert into person (name,age) values(?,?)");
        f.foreach(s => {
          val strs = s.split(",")
          ps.setString(1, strs(0));
          ps.setInt(2, strs(1).toInt);
          ps.executeUpdate();
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
}
