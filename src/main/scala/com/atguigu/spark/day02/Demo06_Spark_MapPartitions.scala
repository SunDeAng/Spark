package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-07 19:42
 * @Desc: mapPartitions()映射
 *        1）函数签名：
 *        def mapPartitions[U: ClassTag](
 *        f: Iterator[T] => Iterator[U],      //f函数把每一个分区的数据分别放入到迭代器中，批处理
 *        preservesPartitioning: Boolean = false): RDD[U]   ////preservesPartitioning：是否保留上游RDD的分区信息，默认false
 *
 *        2）功能说明：Map是一次处理一个元素，而mapPartitions一次处理一个分区数据。
 *
 *        3）需求说明：创建一个RDD，4个元素，2个分区，使每个元素*2组成新的RDD
 *
 */
object Demo06_Spark_MapPartitions {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //3）需求说明：创建一个1-4数组的RDD，两个分区，将所有元素*2形成新的RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val resultRDD: RDD[Int] = rdd.mapPartitions(datas => datas.map(_ * 2))

    //打印，注意必须要有行为函数
    resultRDD.collect().foreach(println)


    //关闭资源
    sc.stop()


  }


}
