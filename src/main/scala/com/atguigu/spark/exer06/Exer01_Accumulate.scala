package com.atguigu.spark.exer06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc: 累加器(分布式共享只写变量)
 *       -前提
 *          Task与Task之间不能读数据
 *       -作用
 *          Task之间的信息聚合
 */
object Exer01_Accumulate {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //0.数据准备
    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    //需求：打印单词出现的次数（a,10）

    //方式一：reduceByKey 代码执行了shuffle
    val res1RDD = dataRDD.reduceByKey(_ + _)
    res1RDD.collect().foreach(println)

    //方式二：使用累加器   不执行shuffle
    //获取累加器变量
    val acc = sc.longAccumulator
    //
    dataRDD.foreach{
      case (key,count) => {
        acc.add(count)
      }
    }
    println(acc.value)


    //方式三：使用自写累加器   不执行shuffle
    //获取累加器
    val accSum = new MyIntAccumulator
    //注册自写累加器
    sc.register(accSum)
    //使用自写累加器
    dataRDD.foreach{
      case (key,count)=>{
        accSum.add(count)
      }
    }
    println(accSum.value)

    //关闭资源
    sc.stop()

  }

}

class MyIntAccumulator extends AccumulatorV2[Int, Int]{

  private var sum = 0

  //是否为初值
  override def isZero: Boolean = sum == 0

  //复制
  override def copy(): AccumulatorV2[Int, Int] = {
    val acc = new MyIntAccumulator
    acc.sum = sum
    acc
  }

  //重置
  override def reset(): Unit = sum = 0

  //累加
  override def add(v: Int): Unit = {
    sum += v
  }

  //聚合
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    this.sum += other.value
  }

  //获取值
  override def value: Int = sum
}


