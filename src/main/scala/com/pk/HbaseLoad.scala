package com.pk

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import com.util.HbaseUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.Text
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

object HbaseLoad {

  def getHJobConf(config: Config): Configuration = {
    val conf = getHbaseConfiguration(config)
    val jobConf = new Configuration(conf)
    jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)
    jobConf
  }

  def getKVPairs(line: Row, cf: String): (ImmutableBytesWritable, Put) = {
    val cfDataBytes = Bytes.toBytes(cf)
    val allFields = line.schema.fields
    val rowkey = Bytes.toBytes(line(0).toString)
    val put = new Put(rowkey)
    for (field <- allFields)
      put.addColumn(cfDataBytes, Bytes.toBytes(field.name), Bytes.toBytes(line.getAs(field.name).toString))

    (new ImmutableBytesWritable(rowkey), put)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Hbase Load").getOrCreate()
    val inConf = ConfigFactory.parseFile(new File(args(0)))
    val path = inConf.getString("inputPath")
    val format = inConf.getString("format")
    val columns = inConf.getStringList("columns").asScala.toList
    val cf = inConf.getString("hbase.column.family")

    val getRdd = spark.read.format(format).load(path).toDF(columns: _*).rdd
    val transformedRDD = getRdd.map(line => getKVPairs(line, cf))
    val hJobConf = getHJobConf(inConf)
    transformedRDD.saveAsNewAPIHadoopDataset(hJobConf)
  }
}
