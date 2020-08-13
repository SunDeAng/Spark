package com.atguigu.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 09:56
 * @Desc:
 *       groupBy  对RDD中的元素按照指定的规则进行分组
 *
 */
object Demo03_groupBy {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //数据准备
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6,7,8,9), 3)

    rdd.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    println("---------------------")

    //按照奇偶数分组
    val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(_ % 2)

    groupRDD.mapPartitionsWithIndex{
      case (index,datas)=>{
        println(index + ":" + datas.mkString(","))
        datas
      }
    }.collect()

    println("---------------------")

    //关闭资源
    sc.stop()

  }

}
