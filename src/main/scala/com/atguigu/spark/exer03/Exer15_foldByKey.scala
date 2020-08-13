package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *      foldByKey  根据key对分区内和分区间的数据进行聚合
 *                 是aggregateByKey的简化，分区内和分区间计算规则相同
 *
 *
 *                默认值   分区内、分区间聚合规则
 *     rdd.foldByKey(0)(_ + _)
 *
 */
object Exer15_foldByKey {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //准备数据
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(("a", 3), ("a", 2), ("c", 4), ("b", 3), ("c", 6), ("c", 8)), 2)
    rdd.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    println("------------------")

    //分区最大值求和
    val resRDD = rdd.foldByKey(0)(_ + _)

    resRDD.mapPartitionsWithIndex(
      (index,datas)=>{
        println(index + "---->"+ datas.mkString(","))
        datas
      }
    ).collect()

    println("------------------")


    //关闭资源
    sc.stop()

  }

}
