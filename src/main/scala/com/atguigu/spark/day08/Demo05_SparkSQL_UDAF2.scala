package com.atguigu.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession, functions}
import org.apache.spark.sql.expressions.{Aggregator, MutableAggregationBuffer, UserDefinedAggregateFunction}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 *       UDF    输入一行，返回一行
 *       UDAF   输入多行，返回一行
 *       UDTF   输入一行，返回多行   SparkSQL中无UDTF
 */
object Demo05_SparkSQL_UDAF2 {

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
    //创建视图
    df.createTempView("people")

    val myAvg2 = new MyAvgUDAF02

    //注册
    spark.udf.register("myAvg",functions.udaf(myAvg2))

    //查询
    spark.sql("select myAvg(age) from people").show()



    //关闭session
    spark.close()

  }

}

/*//输入数据类型
case class User01(username:String,age:Long)
//缓存类型
case class AgeBuffer(var sum:Long,var count:Long)*/

class MyAvgUDAF02 extends Aggregator[Int,AgeBuffer,Double]{
  //设置初始状态
  override def zero: AgeBuffer = AgeBuffer(0L,0L)

  //累加操作
  override def reduce(buf: AgeBuffer, age: Int): AgeBuffer = {
    buf.sum += age
    buf.count += 1
    buf
  }

  override def merge(b1: AgeBuffer, b2: AgeBuffer): AgeBuffer = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  override def finish(buffer: AgeBuffer): Double = {
    buffer.sum.toDouble/buffer.count
  }

  override def bufferEncoder: Encoder[AgeBuffer] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

