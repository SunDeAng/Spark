package com.atguigu.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.AccumulatorV2

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 *       UDF    输入一行，返回一行
 *       UDAF   输入多行，返回一行
 *       UDTF   输入一行，返回多行   SparkSQL中无UDTF
 */
object Demo02_Avg_Accumulate {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名


    //需求：求平均年龄

    //方式二：使用累加器

    val myac = new MyAccumulate01
    spark.sparkContext.register(myac)

    val initRDD = spark.sparkContext.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))

    initRDD.foreach{
      case (name,age)=>{
        myac.add(age)
      }
    }
    println(myac.value)

    //关闭session
    spark.close()

  }

}

class MyAccumulate01 extends AccumulatorV2[Int,Double]{

  private var sum:Int = 0
  private var count:Int = 0

  override def isZero: Boolean =
    {
      sum == 0 && count == 0
    }

  override def copy(): AccumulatorV2[Int, Double] = {
    val myAC = new MyAccumulate01
    myAC.sum = this.sum
    myAC.count = this.count
    myAC

  }

  override def reset(): Unit = {
    sum = 0
    count = 0
  }

  override def add(age: Int): Unit = {
    sum += age
    count += 1
  }

  override def merge(other: AccumulatorV2[Int, Double]): Unit = {
    other match {
      case myac:MyAccumulate01=>{
        sum += myac.sum
        count += myac.count
      }
    }

  }

  override def value: Double = sum.toDouble/count

}
