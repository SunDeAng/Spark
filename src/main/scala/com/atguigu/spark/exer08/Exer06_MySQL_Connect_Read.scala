package com.atguigu.spark.exer08

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Exer06_MySQL_Connect_Read {

  def main(args: Array[String]): Unit = {

    def main(args: Array[String]): Unit = {
      val spark: SparkSession = SparkSession.builder()
        .master("local[*]")
        .appName("sparkSQLDemo")
        .getOrCreate()
      import spark.implicits._


      //方式1：通用的load方法读取
      spark.read.format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/test")
        .option("driver", "com.mysql.jdbc.Driver")
        .option("user", "root")
        .option("password", "123456")
        .option("dbtable", "user")
        .load().show


      /*//方式2
      var prop:Properties = new Properties()
      prop.setProperty("user","root")
      prop.setProperty("password","123456")
      val df: DataFrame = spark.read.jdbc("jdbc:mysql://localhost:3306/test","user",prop)
      df.show()*/

      //方式3
      spark.read.format("jdbc")
        .options(
          Map(
            "url"->"jdbc:mysql://localhost:3306/test?user=root&password=123456",
            "dbtable"->"user",
            "driver"->"com.mysql.jdbc.Driver")
        ).load().show

    }

  }

}
