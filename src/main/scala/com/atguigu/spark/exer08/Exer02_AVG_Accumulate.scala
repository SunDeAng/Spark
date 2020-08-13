package com.atguigu.spark.exer08

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc: 使用累加器求平均年龄
 */
object Exer02_AVG_Accumulate {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //0.创建数据集RDD
    val rdd = sc.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))

    //1.创建自定义累加器对象
    val avg = new MyAvg2
    //2.注册累加器对象
    sc.register(avg)
    //3.使用累加器对象
    rdd.foreach{
      case (name,age)=>{
        avg.add(age)
      }
    }
    //输出结果
    println(avg.value)

    //关闭资源
    sc.stop()

  }

}

class MyAvg2 extends AccumulatorV2[Int,Int]{

   var ageSum = 0
   var count = 0

  override def isZero: Boolean = {
    ageSum == 0 && count ==0
  }

  override def copy(): AccumulatorV2[Int, Int] = {
    val avg = new MyAvg2
    avg.ageSum = this.ageSum
    avg.count = this.count
    avg
  }

  override def reset(): Unit = {
    ageSum = 0
    count = 0
  }

  override def add(age: Int): Unit = {
    this.ageSum += age
    count += 1
  }

  override def merge(other: AccumulatorV2[Int, Int]): Unit = {
    other match {
      case o:MyAvg2 => {
        ageSum += o.ageSum
        count += o.count
      }
      case _ =>
    }
  }

  override def value: Int = ageSum / count
}
