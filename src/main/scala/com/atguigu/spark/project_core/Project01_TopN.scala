package com.atguigu.spark.project_core

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-10
 * @Desc:
 */
object Project01_TopN {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //加载数据
    val rdd: RDD[String] = sc.textFile("E:\\Project\\Spark\\input2")

    //map转换结构 （省份-广告类型，1）
    val mapRDD: RDD[(String, Int)] = rdd.map {
      case (str) => {
        val arr: Array[String] = str.split(" ")
        (arr(1) + "-" + arr(4), 1)
      }
    }

    //reduceByKey聚合 (省份-广告类型，点击总数)
    val reduceRDD = mapRDD.reduceByKey(_ + _)
    reduceRDD.collect().foreach(println)

    //map转换类型   (省份，(广告类型，总数))
    val map1RDD = reduceRDD.map {
      case (data, count) => {
        val arr = data.split("-")
        (arr(0), (arr(1), count))
      }
    }
    //map1RDD.collect().foreach(println)

    //分组
    val groupRDD = map1RDD.groupByKey()
    groupRDD.collect().foreach(println)

    //对当前省份广告点击次数降序排序，取前三
    val result = groupRDD.mapValues(
      datas => datas.toList.sortBy(-_._2).take(3)
    )
    result.collect().foreach(println)

    //关闭资源
    sc.stop()

  }

}
