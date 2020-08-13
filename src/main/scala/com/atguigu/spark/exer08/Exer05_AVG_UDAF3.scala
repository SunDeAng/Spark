package com.atguigu.spark.exer08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Exer05_AVG_UDAF3 {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._

    //创建DataFrame
    val df: DataFrame = spark.read.json("input/test.json")

    //创建临时视图
    df.createOrReplaceTempView("people")

    //创建函数对象
    val myAvg = new MyAvg5
    //注册函数对象
    spark.udf.register("myAvg",functions.udaf(myAvg))

    //使用函数进行查询
    spark.sql("select myAvg(age) from people").show

    //关闭session
    spark.close()

  }

}

//样例类 ： 封装数据，传输数据
case class People05(name:String,age:Long)
//样例类 ： 传输数据，作为缓存
case class AvgBuffer5(var ageSum:Long,var count:Long)

//[IN,Buffer,Out]
class MyAvg5 extends Aggregator[Int,AvgBuffer5,Long]{
  override def zero: AvgBuffer5 = AvgBuffer5(0L,0L)

  override def reduce(b: AvgBuffer5, a: Int): AvgBuffer5 = {
    b.ageSum += a
    b.count += 1
    b
  }

  override def merge(b1: AvgBuffer5, b2: AvgBuffer5): AvgBuffer5 = {
    b1.ageSum += b2.ageSum
    b1.count += b2.count
    b1
  }

  override def finish(reduction: AvgBuffer5): Long = {
    reduction.ageSum/reduction.count
  }

  override def bufferEncoder: Encoder[AvgBuffer5] = Encoders.product

  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
