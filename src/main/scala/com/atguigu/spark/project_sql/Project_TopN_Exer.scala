package com.atguigu.spark.project_sql

import java.text.DecimalFormat

import org.apache.spark.sql.{Encoder, Encoders, SparkSession, functions}
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable

/**
 * @Author: Sdaer
 * @Date: 2020-07-15
 * @Desc:
 */
object Project_TopN_Exer {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Hive-Spark")
      .enableHiveSupport()
      .getOrCreate()

    //创建函数对象
    val cityRemark = new MyRemarkUDAF
    //注册函数
    spark.udf.register("cityRemark",functions.udaf(cityRemark))

    //	查询出来所有的点击记录，并与 city_info 表连接，得到每个城市所在的地区，与 Product_info 表连接得到产品名称
    spark.sql(
      """
        |SELECT pi.product_name ,pi.product_id ,ci.city_name ,ci.area
        |FROM
        |`default`.user_visit_action uva
        |left join
        |`default`.city_info ci
        |on
        |uva.city_id = ci.city_id
        |left join
        |`default`.product_info pi
        |on
        |uva.click_product_id = pi.product_id
        |WHERE
        |uva.click_product_id != -1
        |""".stripMargin).createOrReplaceTempView("t1")

    //	按照地区和商品名称分组，统计出每个商品在每个地区的总点击次数
    spark.sql(
      """
        |SELECT t1.area,t1.product_name,COUNT(*) click_count,cityRemark(t1.city_name)
        |FROM
        |t1
        |group by t1.area,t1.product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    //	每个地区内按照点击次数降序排列
    spark.sql(
      """
        |SELECT
        |t2.*,
        |RANK() over(PARTITION By t2.area SORT By t2.click_count DESC ) click_rank
        |FROM
        |t2
        |""".stripMargin).createOrReplaceTempView("t3")

    //	只取前三名，并把结果保存在数据库中
    spark.sql(
      """
        |select
        |t3.*
        |FROM
        |t3
        |where click_rank<=3
        |""".stripMargin).show(false)

    //	城市备注需要自定义 UDAF 函数

  }

}

//缓存类型      城市点击数  mutable.Map("北京"->1,"天津"->1), 总点击数 Long
case class RemarkBuffer(var cityClickCountMap:mutable.Map[String,Long], var totalCount:Long)

//输出结果的样例类
case class CityRemark(cityName: String, cityRatio: Double) {
  val formatter = new DecimalFormat("0.00%")
  override def toString: String = s"$cityName:${formatter.format(cityRatio)}"
}

class MyRemarkUDAF extends Aggregator[String,RemarkBuffer,String]{
  //设置初始值
  override def zero: RemarkBuffer = {
    RemarkBuffer(mutable.Map[String,Long](),0L)
  }

  //对传入的城市进行聚合
  override def reduce(buf: RemarkBuffer, cityName: String): RemarkBuffer = {
    buf.cityClickCountMap(cityName) = buf.cityClickCountMap.getOrElse(cityName,0L) + 1L
    buf.totalCount += 1
    buf
  }

  //多个缓存区合并
  override def merge(b1: RemarkBuffer, b2: RemarkBuffer): RemarkBuffer = {

    val map1 = b1.cityClickCountMap
    val map2 = b2.cityClickCountMap
    b1.cityClickCountMap = map1.foldLeft(map2)((mm,kv)=>{
      val k = kv._1
      val v = kv._2
      mm(k) = mm.getOrElse(k,0L) + v
      mm
    })
    b1.totalCount += b2.totalCount
    b1

  }

  override def finish(buff: RemarkBuffer): String = {
    val cityMap = buff.cityClickCountMap
    val totalCount = buff.totalCount

    var remarkList = cityMap.toList.sortBy(-_._2).take(2).map {
      case (city, cityCount) => {
        CityRemark(city, cityCount.toDouble / totalCount)
      }
    }

    if (cityMap.size > 2){
      remarkList = remarkList :+ CityRemark("其他", remarkList.foldLeft(1D)(_ - _.cityRatio))

    }
    remarkList.mkString(",")

  }

  override def bufferEncoder: Encoder[RemarkBuffer] = Encoders.product

  override def outputEncoder: Encoder[String] = Encoders.STRING
}
