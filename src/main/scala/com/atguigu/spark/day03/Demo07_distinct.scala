package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc:
 *       distinct 去除rdd中重复的元素
 */
object Demo07_distinct {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 2, 5, 6,5,8,1), 3)
    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()

    println("------------")

    //去重  默认产生的分区与原分区个数相同
    val distinctRDD: RDD[Int] = rdd.distinct()
    distinctRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()

    println("------------")

    //去重  指定分区个数
    val disRDD: RDD[Int] = rdd.distinct(2)
    disRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()

    //关闭资源
    sc.stop()

  }

}
