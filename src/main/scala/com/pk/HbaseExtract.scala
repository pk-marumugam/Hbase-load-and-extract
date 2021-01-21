package com.pk

import java.io.File

import com.typesafe.config.ConfigFactory
import com.pk.util.HbaseUtils.getHbaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

object HbaseExtract {

  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Hbase Extract").getOrCreate()

    val inpConfig = ConfigFactory.parseFile(new File(args(0)))
    val hbTblName = inpConfig.getString("hbase.tblname")
    val cf = inpConfig.getString("hbase.column.family")
    val outputPath = inpConfig.getString("outputPath")
    val format = inpConfig.getString("format")
    val columns = inpConfig.getStringList("columns").asScala.toList

    val hConf = getHbaseConfiguration(inpConfig)
    val HBaseContext = new HBaseContext(spark.sparkContext, hConf)
    val scan = new Scan
    val hbRdd = HBaseContext.hbaseRDD(TableName.valueOf(hbTblName), scan)

    val schema = StructType(List(
      columns.map(c => StructField(c, StringType)): _*
    ))
    val rowRdd = hbRdd.map(x => getResult(x._2, columns, cf))
    val df = spark.createDataFrame(rowRdd, schema)
    df.write.format(format).mode("overwrite").save(outputPath)
  }

  def getResult(value: Result, columns: List[String], cf: String): Row = {
    Row.fromSeq(List(
      columns.map(c => Bytes.toString(value.getValue(Bytes.toBytes(cf), Bytes.toBytes(c)))): _*
    ))
  }

}
