package com.atguigu.spark.exer04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-10
 * @Desc: action行动算子
 *       -reduce()聚合
 *       -foreach()遍历RDD中的每个元素  确定不了Excter执行顺序，打印出为无序
 */
object Exer06_action {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //reduce  对RDD中的元素聚合
    val redRDD = rdd.reduce(_ + _)
    println(redRDD)

    //collect() 以数组形式返回数据集
    val colRDD: Array[Int] = rdd.collect()
    colRDD.foreach(println)


    //foreach()遍历RDD中的每个元素  确定不了Excter执行顺序，打印出为无序
    rdd.foreach(println)


    //count()获取RDD中元素个数
    val count: Long = rdd.count()
    println(count)


    //first() 获取RDD中第一个元素
    val first: Int = rdd.first()
    println("the first of rdd is " + first)
    //take()  获取RDD中前n个元素
    val take: Array[Int] = rdd.take(2)
    println(take.mkString(","))
    //takeOrder() 返回RDD排序后的前n个数
    val rdd1 = sc.makeRDD(List(2, 5, 6, 9, 1, 4))
    val takeOrd = rdd1.takeOrdered(4)
    println(takeOrd.mkString(","))
    //aggregate() 对RDD中的元素进行聚合运算，先计算分区间，再计算分区间
    //第一个参数为初始值 首先会给每个分区分配初始值 用初始值集合分区内计算规则 对区内元素进行聚合
    //分区间在计算的时候 也会有初始值
    val agg = rdd.aggregate(0)(_ + _, _ + _)
    val agg1 = rdd.aggregate(10)(_ + _, _ + _)
    println(agg1)
    //fold()
    val fold = rdd.fold(0)(_ + _)
    println(fold)
    //countByKey()
    val rdd2: RDD[(Int, String)] = sc.makeRDD(List((1, "a"), (1, "a"), (1, "a"), (2, "b"), (3, "c"), (3, "c")))
    val co = rdd2.countByKey()
    println(co)


    //save相关
    rdd.saveAsTextFile("E:\\Project\\Spark\\output")

    //关闭资源
    sc.stop()


  }


}
