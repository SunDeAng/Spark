package com.atguigu.spark.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc: 自定义累加器：统计单词以“H”开头的次数
 */
object Demo02_Accumulate_String {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.makeRDD(List("Hello", "Hello", "Hello", "Hello", "Hello", "Spark", "Spark"))

    val myac = new MyAccumulate
    sc.register(myac)
    rdd.foreach(
      word =>{
        myac.add(word)
      }
    )
    println(myac.value)


    //关闭资源
    sc.stop()

  }

}

class MyAccumulate extends AccumulatorV2[String,mutable.Map[String,Int]]{

  //定义一个Map集合存放结果单词以及出现的次数
  var map = mutable.Map[String,Int]()   //使用可变map，使用（）表示运行apply方法创建


  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[String, mutable.Map[String, Int]] = {
    val myac = new MyAccumulate
    myac.map = this.map
    myac
  }

  //初始化值
  override def reset(): Unit = {
    map.clear()
  }

  //向累加器中添加数据
  override def add(word: String): Unit = {
    //判断当前单词是否以H开头
    if (word.startsWith("H")){
      map(word) = map.getOrElse(word,0) + 1
    }
  }

  //合并
  override def merge(other: AccumulatorV2[String, mutable.Map[String, Int]]): Unit = {
    var map1 = this.map
    var map2 = other.value
    map = map1.foldLeft(map2)((mm,kv)=>{
      val k = kv._1
      val v = kv._2
      mm(k) = mm.getOrElse(k,0) + v
      mm
    })
  }


  override def value: mutable.Map[String, Int] = map
}