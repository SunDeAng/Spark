package com.atguigu.spark.exer06

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc: 广播变量（分布式共享只读变量）
 *
 */
object Exer03_Broadcast {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)


    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val list: List[(String, Int)] = List(("a", 4), ("b", 5), ("c", 6))

    // 声明广播变量
    val broadcastList: Broadcast[List[(String, Int)]] = sc.broadcast(list)

    val resultRDD: RDD[(String, (Int, Int))] = rdd1.map {
      case (k1, v1) => {

        var v2: Int = 0

        // 使用广播变量
        //for ((k3, v3) <- list.value) {
        for ((k3, v3) <- broadcastList.value) {
          if (k1 == k3) {
            v2 = v3
          }
        }

        (k1, (v1, v2))
      }
    }
    resultRDD.foreach(println)


    //关闭资源
    sc.stop()

  }

}
