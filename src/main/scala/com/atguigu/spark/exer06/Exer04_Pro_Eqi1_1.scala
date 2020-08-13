package com.atguigu.spark.exer06

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc: 需求一：热门品类Top10
 *       老师做法一
 */
object Exer04_Pro_Eqi1_1 {

  def main(args: Array[String]): Unit = {

    //创建Spark配置文件，加载配置信息
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Demo")

    //创建SparkContext对象
    val sc = new SparkContext(conf)

    //加载数据
    val lineRDD = sc.textFile("input3")

    //数据切分
    val userVisitAction = lineRDD.map {
      line => {
        val field = line.split("_")
        UserVisitAction(
          field(0),
          field(1).toLong,
          field(2),
          field(3) toLong,
          field(4),
          field(5),
          field(6) toLong,
          field(7) toLong,
          field(8),
          field(9),
          field(10),
          field(11),
          field(12) toLong
        )
      }
    }

    //数据清洗  切成(品类id,点击1,0,0)  (品类id,0,下单1,0) (品类id,0,0,支付1)
    val flatMapRDD = userVisitAction.flatMap {
      case uva => {
        var list = new ListBuffer[CategoryCountInfo]()
        if (uva.click_category_id != -1) {
          list.append(CategoryCountInfo(uva.click_category_id.toString, 1, 0, 0))
        } else if (uva.order_category_ids != "null") {
          val userOrder = uva.order_category_ids.split(",")
          userOrder.foreach(
            str => list.append(CategoryCountInfo(str, 0, 1, 0))
          )
        } else if (uva.pay_category_ids != "null") {
          val userPay = uva.pay_category_ids.split(",")
          userPay.foreach(
            str => list.append(CategoryCountInfo(str, 0, 0, 1))
          )
        }else{
          Nil
        }
        list
      }
    }

    //分组统计
    val groupRDD = flatMapRDD.groupBy(
      cci => cci.categoryId
    )

    //统计排序取值
    val reduceRDD = groupRDD.mapValues {
      datas => {
        datas.reduce {
          (info1, info2) => {
            info1.clickCount = 0L + info1.clickCount + info2.clickCount
            info1.orderCount = 0L + info1.orderCount + info2.orderCount
            info1.payCount = 0L + info1.payCount + info2.payCount
            info1
          }
        }
      }
    }

    //转换结构
    val mapRDD = reduceRDD.map {
      _._2
    }

    //排序输出
    val sortRDD = mapRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false)

    sortRDD.take(10).foreach(println)


    //关闭资源
    sc.stop()

  }

}

//用户访问动作表
case class UserVisitAction(date: String,//用户点击行为的日期
                           user_id: Long,//用户的ID
                           session_id: String,//Session的ID
                           page_id: Long,//某个页面的ID
                           action_time: String,//动作的时间点
                           search_keyword: String,//用户搜索的关键词
                           click_category_id: Long,//某一个商品品类的ID
                           click_product_id: Long,//某一个商品的ID
                           order_category_ids: String,//一次订单中所有品类的ID集合
                           order_product_ids: String,//一次订单中所有商品的ID集合
                           pay_category_ids: String,//一次支付中所有品类的ID集合
                           pay_product_ids: String,//一次支付中所有商品的ID集合
                           city_id: Long)//城市 id
// 输出结果表
case class CategoryCountInfo(var categoryId: String,//品类id
                             var clickCount: Long,//点击次数
                             var orderCount: Long,//订单次数
                             var payCount: Long)//支付次数
