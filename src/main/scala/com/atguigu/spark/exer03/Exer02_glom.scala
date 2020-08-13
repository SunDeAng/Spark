package com.atguigu.spark.exer03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-08 11:53
 * @Desc:
 *       glom     : 将一个RDD分区中的每个元素放在一个数组中
 *       flatMap  : 对RDD中的元素进行扁平化处理
 *
 *
 */
object Exer02_glom {

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


    //分区求最大值求和
    //将集合中的数据进行数组整合
    val glomRDD: RDD[Array[Int]] = rdd.glom()
    //求每个分区中的最大值
    val maxRDD: RDD[Int] = glomRDD.map(_.max)
    //将每个分区最大值进行求和
    val result = maxRDD.sum()

    println("和为"+ result)



    //关闭资源
    sc.stop()

  }

}
