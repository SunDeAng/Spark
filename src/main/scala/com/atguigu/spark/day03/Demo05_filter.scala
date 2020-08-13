package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc:
 *       filter 对RDD中的元素进行过滤
 */
object Demo05_filter {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    println("-----------------------")

    //安装偶数规则过滤，即取出集合中为偶数的元素
    val filterRDD: RDD[Int] = rdd.filter((_ % 2 == 0))
    filterRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    //关闭资源
    sc.stop()

  }

}
