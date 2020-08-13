package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc:
 */
object Demo09_repartition {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6,7,8,9), 3)

    //实际在扩大分区用repartition算子   底层调用coalesce并且默认使用shuffle
    val reRDD = rdd.repartition(2)
    reRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()


    //实际在扩大分区用repartition算子   底层调用coalesce并且默认使用shuffle
    val redRDD = rdd.repartition(4)
    redRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + "-->" + datas.mkString(","))
        datas
      }
    }.collect()



    //关闭资源
    sc.stop()

  }

}
