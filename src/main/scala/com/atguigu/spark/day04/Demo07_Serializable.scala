package com.atguigu.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-10
 * @Desc:
 *   为什么要序列化
 *       因为Spark程序初始化操作发生在Driver端，具体算子的执行是在Executor端执行
 *       如果在Executor执行的时候，要访问Driver端的数据，那么就涉及跨进程或者跨节点之间的同学，
 *       所以要求传递的数据必须是可序列化的
 *   如何确定
 */
object Demo07_Serializable {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val ydcx: User = new User
    ydcx.name = "ydcx"

    val tnl: User = new User
    tnl.name = "tnl"

    val userRDD: RDD[User] = sc.makeRDD(List(ydcx,tnl))

    userRDD.foreach(println(_))

    //关闭资源
    sc.stop()

  }

}

class User {
  var name:String = _

  override def toString = s"User($name)"
}
