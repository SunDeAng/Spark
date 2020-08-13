package com.atguigu.spark.day08

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Demo07_Mysql_write {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._

    val df = spark.read.json("input/test.json")

    val ds = df.as[People01]

    //方式1：通用的方式  format指定写出类型
      ds.write
        .format("jdbc")
        .option("url", "jdbc:mysql://hadoop202:3306/test")
        .option("user", "root")
        .option("password", "123456")
        .option("dbtable", "user")
        .mode(SaveMode.Append)
        .save()

    //方式2：通过jdbc方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    ds.write.mode(SaveMode.Append).jdbc("jdbc:mysql://hadoop202:3306/test", "user", props)


    //关闭session
    spark.close()

  }

}

case class People01(name:String,age:Long)
