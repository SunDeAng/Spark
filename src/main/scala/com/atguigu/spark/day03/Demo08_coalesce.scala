package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc: 重新分区
 *
 *       -coalesce
 *          默认不shuffle
 *          一般用于缩减
 *       -repartition
 *          底层调用coalesce，并且使用shuffle参数进行重新分区
 *          一般用于扩大分区
 */
object Demo08_coalesce {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6,7,8,9), 3)
    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()

    println("------------")
/*

    //缩减分区  coalesce算子，默认没有shuffle
    val coalRDD = rdd.coalesce(2)
    coalRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()

    println("------------")
*/
/*
    //扩大分区  coalesce算子，没有shuffle，不会重新打乱重组，扩大分区无意义，故使用此算子没扩大分区
    val coalRDD = rdd.coalesce(4)
    coalRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()

    */

    //扩大分区  coalesce算子，使用参数执行shuffle
    val coalRDD = rdd.coalesce(4,true)
    coalRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------")

    //实际在扩大分区用repartition算子   底层调用coalesce并且默认使用shuffle
    val reRDD = rdd.repartition(2)
    reRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()


    //关闭资源
    sc.stop()

  }

}
