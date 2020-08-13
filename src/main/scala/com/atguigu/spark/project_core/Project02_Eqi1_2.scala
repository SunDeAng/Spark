package com.atguigu.spark.project_core

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @Author: Sdaer
 * @Date: 2020-07-13
 * @Desc:
 */
object Exer04_Pro_Eqi1_2 {

  def main(args: Array[String]): Unit = {
    //1.创建SparkConf并设置App名称
    val conf: SparkConf = new SparkConf().setAppName("SparkCoreTest").setMaster("local[*]")

    //2.创建SparkContext，该对象是提交Spark App的入口
    val sc: SparkContext = new SparkContext(conf)

    // 读取数据
    val dataRDD: RDD[String] = sc.textFile("input3")

    //将原始数据进行转换(分解)
    val actionRDD: RDD[UserVisitAction] = dataRDD.map {
      line => {
        val fields: Array[String] = line.split("_")
        UserVisitAction(
          fields(0),
          fields(1).toLong,
          fields(2),
          fields(3).toLong,
          fields(4),
          fields(5),
          fields(6).toLong,
          fields(7).toLong,
          fields(8),
          fields(9),
          fields(10),
          fields(11),
          fields(12).toLong
        )
      }
    }

    //3.3 创建累加器
    val acc: CategoryCountAccumulator = new CategoryCountAccumulator()

    //3.4 注册累加器
    sc.register(acc, "CategoryCountAccumulator")

    actionRDD.foreach(
      action => {
        acc.add(action)
      }
    )

    //3.5 获取累加器的值
    //((鞋,click),10)
    //((鞋,order),20)
    //((鞋,pay),30)
    val accMap: mutable.Map[(String, String), Long] = acc.value

    //对累加出来的数据按照类别进行分组，注意：这个时候每个类别之后有三条记录，数据量不会很大
    val groupMap: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)

    //对分组后的数据进行结构的转换:CategoryCountInfo
    val infoes: immutable.Iterable[CategoryCountInfo] = groupMap.map {
      case (id, map) => {
        CategoryCountInfo(
          id,
          map.getOrElse((id, "click"), 0L),
          map.getOrElse((id, "order"), 0L),
          map.getOrElse((id, "pay"), 0L)
        )
      }
    }
    //将转换后的数据进行排序（降序）取前10名
    infoes.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {

          if (left.orderCount > right.orderCount) {
            true
          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }

        } else {
          false
        }
      }
    ).take(10).foreach(println)



    // 关闭连接
    sc.stop()
  }


}

class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{
  var map = mutable.Map[(String,String),Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    var newMap = new CategoryCountAccumulator
    newMap.map = this.map
    newMap
  }

  override def reset(): Unit = {
    map.clear()
  }

  //((鞋,click),1)
  override def add(action: UserVisitAction): Unit = {
    if(action.click_category_id != -1){
      //点击
      var key = (action.click_category_id.toString,"click")
      map(key) = map.getOrElse(key,0L) + 1L
    }else if(action.order_category_ids != "null"){
      //下单
      val ids: Array[String] = action.order_category_ids.split(",")
      for (id <- ids) {
        var key = (id,"order")
        map(key) = map.getOrElse(key,0L) + 1L
      }
    }else if(action.pay_category_ids != "null"){
      //支付
      val ids: Array[String] = action.pay_category_ids.split(",")
      for (id <- ids) {
        var key = (id,"pay")
        map(key) = map.getOrElse(key,0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    var map1 = map
    var map2 = other.value
    map = map1.foldLeft(map2)(
      (mmpp,kv)=>{
        mmpp(kv._1) = mmpp.getOrElse(kv._1,0L) + kv._2
        mmpp
      }
    )
  }

  override def value: mutable.Map[(String, String), Long] = map
}

