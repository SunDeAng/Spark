package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-07 19:42
 * @Desc: mapPartitions()映射
 *       1)函数签名
 *        def mapPartitionsWithIndex[U: ClassTag](
 *        f: (Int, Iterator[T]) => Iterator[U],
 *        preservesPartitioning: Boolean = false): RDD[U]
 *
 *        2）功能说明：类似于mapPartitions，比mapPartitions多一个整数参数表示分区号
 *
 *        3）需求说明：需求说明：创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD
 *
 */
object Demo07_Spark_MapPartitionsWithIndex {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //3）需求说明：创建一个RDD，使每个元素跟所在分区号形成一个元组，组成一个新的RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val resultRDD: RDD[(Int, Int)] = rdd.mapPartitionsWithIndex((index, items) => {
      items.map((index, _))
    })

    //扩展功能：第二个分区元素*2，其余分区不变
    val newRDD: RDD[Int] = rdd.mapPartitionsWithIndex {
      case (index, datas) => {
        index match {
          case 2 => datas.map(_ * 2)
          case _=> datas
        }
      }
    }
    newRDD.collect().foreach(println)

    //打印，注意必须要有行为函数
    resultRDD.collect().foreach(println)


    //关闭资源
    sc.stop()


  }


}
