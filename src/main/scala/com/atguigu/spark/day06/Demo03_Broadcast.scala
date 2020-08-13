package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc:
 */
object Demo03_Broadcast {

  def main(args: Array[String]): Unit = {
    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("b", 2), ("c", 3)))
    val list: List[(String, Int)] = List(("a", 4), ("b", 5), ("c", 6))

    val resRDD = rdd1.map {
      case (k1, v1) => {
        var v3 = 0
        for ((k2, v2) <- list) {
          if (k1 == k2) {
            v3 = v2
          }

        }
        (k1, (v1, v3))
      }
    }
    resRDD.collect().foreach(println)

    //创建广播变量
    val broc = sc.broadcast(list)
    val res1RDD = rdd1.map {
      case (k1, v1) => {
        var v3 = 0
        for ((k2, v2) <- broc.value) {
          if (k1 == k2) {
            v3 = v2
          }

        }
        (k1, (v1, v3))
      }
    }
    res1RDD.collect().foreach(println)




    //关闭资源
    sc.stop()
  }

}
