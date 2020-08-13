package com.atguigu.spark.exer08

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StructField, StructType}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc: 自定义UDAF实现AVG
 *       此实现为弱类型实现
 *       旧版若类型，用于SparkSql
 */
object Exer03_AVG_UDAF1 {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._

    //创建自定义函数对象
    val avg = new MyAvg3
    //注册对象
    spark.udf.register("avgAge",avg)

    //加载数据
    val df = spark.read.json("input/test.json")
    //创建临时视图
    df.createTempView("people")

    //使用自定义查询
    spark.sql("select avgAge(age) from people").show()

    //关闭session
    spark.close()

  }

}

//此继承的类在本版本过期，有替代方案实现
class MyAvg3 extends UserDefinedAggregateFunction{

  //输入参数数据类型
  override def inputSchema: StructType = {
    StructType(List(StructField("age",IntegerType)))
  }

  //缓存参数数据类型
  override def bufferSchema: StructType = {
    StructType(List(StructField("ageSum",IntegerType),StructField("count",IntegerType)))
  }

  //输出参数数据类型
  override def dataType: DataType = IntegerType

  //校验检查
  override def deterministic: Boolean = true

  //缓冲器初始化
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0   //缓冲区   ageSum为0
    buffer(1) = 0   //缓冲区   count为0
  }

  //更新缓冲区数据
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getInt(0) + input.getInt(0)
    buffer(1) = buffer.getInt(1) + 1

  }

  //数据聚合
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getInt(0) + buffer2.getInt(0)
    buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
  }

  //数据输出
  override def evaluate(buffer: Row): Any = buffer.getInt(0)/buffer.getInt(1)
}

