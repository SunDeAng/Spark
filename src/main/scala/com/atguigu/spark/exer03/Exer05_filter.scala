package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *       filter 过滤
 *
 *
 */
object Exer05_filter {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd = sc.makeRDD(List(1, 2, 3, 4,5,6,7,8,9),3)
    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()
    println("------------------")


    //得到偶数
    val filterRDD = rdd.filter(_ % 2 == 0)

    filterRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index+ "-->"+ datas.mkString(","))
        datas
      }
    }.collect()


    //关闭资源
    sc.stop()

  }

}
