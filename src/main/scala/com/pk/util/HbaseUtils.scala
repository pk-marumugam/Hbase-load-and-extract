package com.pk.util

import com.typesafe.config.Config
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

object HbaseUtils {

  def getHbaseConfiguration(config: Config): Configuration = {
    val conf = HBaseConfiguration.create()
    config.getStringList("hbase.source.files").asScala.toList.foreach(file => conf.addResource(new Path(file)))
    val hbTblName = config.getString("hbase.tblname")
    val zkpQuorum = config.getString("hbase.zookeeper.quorum")
    conf.set("hbase.zookeeper.quorum", zkpQuorum)
    conf.set(TableOutputFormat.OUTPUT_TABLE, hbTblName)
    conf
  }

  def getHTable(config: Config): (Table, Connection) = {
    val conf = HBaseConfiguration.create()
    config.getStringList("hbase.files").asScala.toList.foreach(file => conf.addResource(new Path(file)))
    val connection = ConnectionFactory.createConnection(conf)
    val hbTblName = config.getString("hbase.tblname")
    val con = Try {
      connection.getTable(TableName.valueOf(hbTblName))
    }
    val hTable = con match {
      case Success(s) => s
      case Failure(f) => null
    }
    (hTable, connection)
  }
}
