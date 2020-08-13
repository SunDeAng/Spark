package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:24
 * @Desc: 转换算子
 *       glom 将一个RDD中分区中的每个元素
 */
object Demo02_glom {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)
    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    println("------------------------")

    val newRDD: RDD[Array[Int]] = rdd.glom()
    //newRDD.collect().foreach(_.foreach(println))
    newRDD.collect().foreach(arr=> println(arr.mkString(",")))


    //需求：分区最大值求和
    val rdd1: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)
    //将rdd中每个分区中的元素合并到一个数组中
    val glomRDD: RDD[Array[Int]] = rdd1.glom()
    //取出每个数组中最大的数字
    val maxRDD: RDD[Int] = glomRDD.map(_.max)
    //将数组中的数字求和
    val sumValue = maxRDD.sum()
    println(sumValue)



    //关闭资源
    sc.stop()

  }

}
