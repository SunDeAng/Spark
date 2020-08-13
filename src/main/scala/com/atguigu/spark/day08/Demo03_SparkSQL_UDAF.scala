package com.atguigu.spark.day08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StructField, StructType}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 *       UDF    输入一行，返回一行
 *       UDAF   输入多行，返回一行
 *       UDTF   输入一行，返回多行   SparkSQL中无UDTF
 */
object Demo03_SparkSQL_UDAF {

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

    //创建UDAF对象
    val myAvg = new MyAvgUDAF
    //注册对象
    spark.udf.register("myAvg",myAvg)

    //使用函数查询
    spark.sql("select myAvg(age) from people").show()


    //关闭session
    spark.close()

  }

}

//3.0.0过时方法
class MyAvgUDAF extends UserDefinedAggregateFunction{

  //输入数据类型
  override def inputSchema: StructType = StructType(List(StructField("age",IntegerType)))

  //缓存数据类型
  override def bufferSchema: StructType = {
    StructType(List(StructField("ageSum",LongType),StructField("count",LongType)))
  }


  //输出数据类型
  override def dataType: DataType = DoubleType

  //精准性校验
  override def deterministic: Boolean = true

  //设置缓存初始状态
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //更新缓存状态
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val age = input.getInt(0)
    buffer(0) = buffer.getLong(0) + age
    buffer(1) = buffer.getLong(1) + 1L

  }

  //合并缓存数据
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //返回输出结果
  override def evaluate(buffer: Row): Double = buffer.getLong(0).toDouble/buffer.getLong(1)
}
