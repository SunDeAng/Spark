package com.atguigu.spark.project_core

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc:
 */
object Project02_Eqi2_1 {

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
    val mapRDD = reduceRDD.map (_._2)

    //排序输出
    val hotTop10 = mapRDD.sortBy(info => (info.clickCount, info.orderCount, info.payCount), false).take(10)




    //需求二   对于排名前10的品类，分别获取每个品类点击次数排名前10的sessionId


    //	通过需求1，获取TopN热门品类的id
    val ids: Array[String] = hotTop10.map(_.categoryId)
    //因为这个ids要分给每个任务，所以可以使用广播变量
    val broadcastIds: Broadcast[List[String]] = sc.broadcast(ids.toList)
    //	将原始数据进行过滤（1.保留热门品类 2.只保留点击操作）
    val filterRDD = userVisitAction.filter(
      action => {
        if (action.click_category_id != -1) {
          broadcastIds.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      }
    )
    //	对session的点击数进行转换 (category-session,1)
    val map21RDD = filterRDD.map(
        uva => {
          (uva.click_category_id + "_" + uva.session_id, 1)
        }
    )
    //	对session的点击数进行统计 (category-session,sum)
    val reduceRDD21 = map21RDD.reduceByKey(_ + _)
    //	将统计聚合的结果进行转换  (category,(session,sum))
    val map22RDD = reduceRDD21.map {
      datas => {
        val tmpArray = datas._1.split("_")
        (tmpArray(0), (tmpArray(1), datas._2))
      }
    }
    //	将转换后的结构按照品类进行分组 (category,Iterator[(session,sum)])
    val group21RDD = map22RDD.groupByKey()
    //	对分组后的数据降序 取前10
/*    val sort21RDD = group21RDD.mapValues(
        datas => {
          datas.toList.sortBy( -_._2)
        }
    )*/
    val sort21RDD: RDD[(String, List[(String, Int)])] = group21RDD.mapValues {
      datas => {
        datas.toList.sortWith {
          case (left, right) => {
            left._2 > right._2
          }
        }.take(10)
      }
    }
    sort21RDD.take(10).foreach(println)


    //关闭资源
    sc.stop()

  }

}
