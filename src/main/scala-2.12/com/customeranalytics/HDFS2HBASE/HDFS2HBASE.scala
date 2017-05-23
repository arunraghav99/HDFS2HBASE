package com.customeranalytics.HDFS2HBASE

/**
  * Created by Arunkumar.D on 5/3/2017.
  */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes

import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf

object HDFS2HBASE extends Serializable {
  val tableName = "Item_series"
  val cfDataBytes = Bytes.toBytes("ISP")
  val colItBytes = Bytes.toBytes("item")
  val colserBytes = Bytes.toBytes("series")

  case class ITEM_SERIES(item: String, series: String)

  object ISP_SERIES extends Serializable {
    def parseITEM_SERIES(str: String): ITEM_SERIES = {
      val p = str.split("\t")
      ITEM_SERIES(p(0), p(1))
    }

    def convertToPut(itemseries: ITEM_SERIES): (ImmutableBytesWritable, Put) = {
      val rowkey = itemseries.item + "" + itemseries.series
      val put = new Put(Bytes.toBytes(rowkey))
      put.addColumn(cfDataBytes, colItBytes, Bytes.toBytes(itemseries.item))
      put.addColumn(cfDataBytes, colserBytes, Bytes.toBytes(itemseries.series))
      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)),put)
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE,tableName)
    val jobConfig: JobConf = new JobConf(conf, this.getClass)
    jobConfig.set("mapreduce.output.fileoutputformat.outputdir", "/user/user01/out")
    jobConfig.setOutputFormat(classOf[TableOutputFormat])
    jobConfig.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val sc = new SparkContext
    val inputData = sc.textFile("hdfs://sandbox.hortonworks.com:8020/tmp/sample2/test.csv")

    inputData.foreach { rdd =>
      rdd.map(ISP_SERIES.convertToPut())

    }

  }
}