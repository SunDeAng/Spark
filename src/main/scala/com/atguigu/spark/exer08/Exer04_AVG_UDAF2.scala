package com.atguigu.spark.exer08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SparkSession, TypedColumn}
import org.apache.spark.sql.expressions.Aggregator

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc: 新版强类型 用于DSL语言
 */
object Exer04_AVG_UDAF2 {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._

    //加载数据
    val df = spark.read.json("input/test.json")
    //转换成ds
    val ds = df.as[People04]

    //创建自定义函数的对象
    val myAvg = new MyAvg4

    val column: TypedColumn[People04, Long] = myAvg.toColumn

    //使用自定义函数
    ds.select(column).show()


    //关闭session
    spark.close()

  }

}

//样例类 ： 封装数据，传输数据
case class People04(name:String,age:Long)
//样例类 ： 传输数据，作为缓存
case class AvgBuffer(var ageSum:Long,var count:Long)

//[IN,Buffer,Out]
class MyAvg4 extends Aggregator[People04,AvgBuffer,Long]{
  //设置缓存初试值
  override def zero: AvgBuffer = {
    //初始值全设置为0
    AvgBuffer(0L,0L)
  }

  //数据聚合
  override def reduce(buff: AvgBuffer, a: People04): AvgBuffer = {
    //数据聚合
    buff.ageSum += a.age
    buff.count += 1L
    //将聚合后的缓冲对象返回
    buff
  }

  //合并
  override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
    b1.ageSum += b2.ageSum
    b1.count += b2.count
    b1
  }

  //返回结果
  override def finish(buff: AvgBuffer): Long = {
    buff.ageSum/buff.count
  }

  //缓存编码  默认使用Encoders.product
  override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product

  //输出编码  根据输出数据类型指定
  override def outputEncoder: Encoder[Long] = Encoders.scalaLong
}
