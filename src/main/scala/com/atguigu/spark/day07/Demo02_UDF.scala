package com.atguigu.spark.day07

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: Sdaer
 * @Date: 2020-07-14
 * @Desc:
 */
object Demo02_UDF {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._

    //注册函数
    spark.udf.register("sayHi",(name:String)=>{"hello-->"+name})

    //读取json创建df
    val df = spark.read.json("input/test.json")

    //创建临时视图
    df.createTempView("people")

    //使用udf
    spark.sql("select sayHi(name) from people").show()

    //关闭session
    spark.close()



  }

}
