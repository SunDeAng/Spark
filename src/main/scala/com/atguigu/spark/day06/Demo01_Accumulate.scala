package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc: 分布式共享只写变量
 */
object Demo01_Accumulate {

  def main(args: Array[String]): Unit = {
    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)


    val dataRDD: RDD[(String, Int)] = sc.makeRDD(List(("a", 1), ("a", 2), ("a", 3), ("a", 4)))

    /*
    val resRDD = dataRDD.reduceByKey(_ + _)

    resRDD.collect().foreach(println)
    */

    /*
    //定义变量接收出现次数和
    var sum:Int = 0
    dataRDD.foreach{
      case (_,count)=>{
        sum += count
      }
    }
    println(sum)    //sum==0
    */

/*

    //使用累加器
    val sum: LongAccumulator = sc.longAccumulator
    dataRDD.foreach{
      case (_,count)=>{
        sum.add(count)
        println(sum.value)
      }
    }
    println(sum.value)

*/


    //使用自定义累加器
    //创建对象
    val myac = new MyLongAccumulator
    //注册累加器
    sc.register(myac)
    dataRDD.foreach{
      case (_,count)=>{
        myac.add(count)
        println(myac.value)
      }
    }
    println(myac.value)


    //关闭资源
    sc.stop()
  }

}

class MyLongAccumulator extends AccumulatorV2[Int, Int] {

  private var sum = 0
  //private var _count = 0

  //判断是否为初始值
  override def isZero: Boolean = sum==0

  //拷贝
  override def copy(): AccumulatorV2[Int, Int] = {
    val myac = new MyLongAccumulator
    myac.sum = this.sum
    myac
  }

  //恢复初始状态
  override def reset(): Unit = sum=0

  //累加
  override def add(v: Int): Unit = {
    sum += v
  }

  //聚合，合并各个Task数据
  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    this.sum += other.value
  }

  //获取累加器的值
  override def value: Int = sum
}
