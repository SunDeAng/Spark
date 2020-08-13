package com.atguigu.spark.day09

import java.net.Socket

import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-17
 * @Desc: 自定义数据源创建DS
 */
object Demo03_Create_Customer {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Test")
    val ssc = new StreamingContext(conf, Seconds(3))

    //自定义数据源
    //ssc.receiverStream()

  }

}

class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  private var socket:Socket = _

  //开始读取
  override def onStart(): Unit = {

  }

  //停止读取
  override def onStop(): Unit = {
    synchronized()
  }
}
