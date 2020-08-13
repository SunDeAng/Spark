package com.atguigu.spark.exer06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc:
 */
object Exer02_MyAccumulate {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"))

    //需求：自定义累加器，统计集合中首字母为“H”单词出现的次数。

    val acc = new MyAccumulate
    sc.register(acc)
    rdd.foreach(
      word => {
        acc.add(word)
      }
    )
    println(acc.value)

    //关闭资源
    sc.stop()

  }

}

class MyAccumulate extends AccumulatorV2[String,mutable.Map[String,Long]]{

  //定义输出数据集合
  var map: mutable.Map[String, Long] = mutable.Map[String, Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String,mutable.Map[String,Long]] = {
    new MyAccumulate
  }

  override def reset(): Unit = map.clear()

  override def add(v: String): Unit = {
    if (v.startsWith("H")){
      map(v) = map.getOrElse(v,0L) + 1L
    }
  }

  override def merge(other: AccumulatorV2[String,mutable.Map[String,Long]]): Unit = {

    var map1 = map;
    var map2 = other.value

    map = map1.foldLeft(map2)(
      (map,kv)=>{
        map(kv._1) = map.getOrElse(kv._1,0L) + kv._2
        map
      }
    )

  }

  override def value:mutable.Map[String,Long]= map
}
