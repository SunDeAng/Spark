package com.atguigu.spark.exer09

import java.io.{BufferedReader, InputStreamReader}
import java.net.{ConnectException, Socket}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-17
 * @Desc: 自定义数据源，实现监控某个端口号，获取该端口号内容
 */
object Exer03_Create_Custom {

  def main(args: Array[String]): Unit = {

    //创建配置信息
    val conf = new SparkConf().setAppName("SparkSteaming").setMaster("local[*]")
    //常见SparkStreaming的入口StreamingContext
    //第二个参数为时间间隔
    val ssc = new StreamingContext(conf, Seconds(3))


    //自定义数据源  创建DStream
    val lineDS = ssc.receiverStream(new MyReceiver("hadoop102", 9999))

    //处理数据
    lineDS
      .flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_+_)
      .print


    //开始采集数据
    ssc.start()
    ssc.awaitTermination()
  }

}

//自定义类  继承Receiver
//可以类比SocketReceiver类来写
class MyReceiver(host:String,port:Int) extends Receiver[String](StorageLevel.MEMORY_ONLY){

  //创建端口号变量
  private var socket:Socket = _

  override def onStart(): Unit = {
    try{
      //将主机及端口号绑定给socket连接
      socket = new Socket(host,port)
    }catch {
      case e:ConnectException=>{
        //???????????
        restart(s"Error connecting to $host:$port",e)
        return
      }
    }

    new Thread("Socket Receiver"){
      setDaemon(true)
      override def run():Unit = {receive()}
    }.start()

  }

  //信息接收函数
  def receive(): Unit ={

    //读取指定端口的数据
    //获取的是最基础的输入流，我们需要对其进行包装来使用读取行数据方法
    val bf = new BufferedReader(new InputStreamReader(socket.getInputStream))

    //定义一个变量，存储读取到的一行的数据
    var line:String = null

    //循环获取行数据
    while ((line = bf.readLine())!=null){
      //父类Receiver提供的保存到缓存的方法
      store(line)
    }

  }


  override def onStop(): Unit = {
    // in case restart thread close it twice
    synchronized {
      if (socket != null) {
        socket.close()
        socket = null
      }
    }
  }
}
