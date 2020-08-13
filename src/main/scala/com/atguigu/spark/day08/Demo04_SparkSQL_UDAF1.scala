package com.atguigu.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 *       UDF    输入一行，返回一行
 *       UDAF   输入多行，返回一行
 *       UDTF   输入一行，返回多行   SparkSQL中无UDTF
 */
object Demo04_SparkSQL_UDAF1 {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._


    //需求：求平均年龄

    //方式三：使用UDAF(弱类型)实现   一般应用于SQL风格
    val initRDD = spark.sparkContext.makeRDD(List(("zhangsan", 20), ("lisi", 30), ("wangw", 40)))

    //创建DF
    val df = spark.read.json("input/test.json")
    //转为DS
    val ds = df.as[User01]





    //关闭session
    spark.close()

  }

}

//输入数据类型
case class User01(username:String,age:Long)
//缓存类型
case class AgeBuffer(var sum:Long,var count:Long)

class MyAvgUDAF01 extends Aggregator[User01,AgeBuffer,Double]{
  override def zero: AgeBuffer = AgeBuffer(0L,0L)

  override def reduce(b: AgeBuffer, a: User01): AgeBuffer = {
    b.sum += a.age
    b.count += 1
    b
    
  }

  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(buff: AgeBuffer): Double = {
    buff.sum.toDouble/buff.count
  }

  override def bufferEncoder: Encoder[AgeBuffer] = {
    Encoders.product
  }

  override def outputEncoder: Encoder[Double] = {
    Encoders.scalaDouble
  }
}

