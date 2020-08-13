package com.atguigu.spark.exer08

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Exer07_MySQL_Write {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("sparkSQLDemo")
      .getOrCreate()

    import spark.implicits._
    //创建df
    val df: DataFrame = spark.read.json("input/test.json")

    //df.write.format("jdbc")
    //方式1：通用的方式  format指定写出类型
    val ds: Dataset[People07] = df.as[People07]
    ds.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/test")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .mode(SaveMode.Append)
      .save()

    /*//方式2：通过jdbc方法
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")
    df.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/test", "user", props)
*/

  }

}
case class People07(name:String,age:Long)
