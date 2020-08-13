package com.atguigu.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-07 19:42
 * @Desc: map()映射
 *       功能
 *          参数f是一个函数，它可以接收一个参数。
 *          当某个RDD执行map方法时，会遍历该RDD中的每一个数据项，并依次应用f函数，从而产生一个新的RDD。即，这个新RDD中的每一个元素都是原来RDD中每一个元素依次应用f函数而得到的。
 */
object Demo05_Spark_Map {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //3）需求说明：创建一个1-4数组的RDD，两个分区，将所有元素*2形成新的RDD
    val rdd: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4), 2)

    val resultRDD: RDD[Int] = rdd.map(_ * 2)

    //打印，注意必须要有行为函数
    resultRDD.collect().foreach(println)


    //关闭资源
    sc.stop()


  }


}
