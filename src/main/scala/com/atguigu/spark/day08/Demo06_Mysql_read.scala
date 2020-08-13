package com.atguigu.spark.day08

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Demo06_Mysql_read {

  def main(args: Array[String]): Unit = {

    //创建配置文件
    val conf = new SparkConf().setMaster("local[*]").setAppName("SparkSession")
    //创建session对象
    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
    //导入隐式转换规则,spark不是包，是SparkSession的别名
    import spark.implicits._

    //方式一
    /*val url = "jdbc:mysql://localhost:3306/test"
    val prop = new Properties()
    prop.setProperty("user","root")
    prop.setProperty("password","123456")

    val df = spark.read.jdbc(url, "user", prop)
    df.show()*/

    //方式2：通用的load方法读取
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load().show

    //方式3:通用的load方法读取 参数另一种形式
    spark.read.format("jdbc")
      .options(Map("url"->"jdbc:mysql://localhost:3306/test?user=root&password=123456",
        "dbtable"->"user","driver"->"com.mysql.jdbc.Driver")).load().show




    //关闭session
    spark.close()

  }

}
