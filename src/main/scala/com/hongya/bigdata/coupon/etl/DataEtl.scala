package com.hongya.bigdata.coupon.etl

import com.hongya.bigdata.coupon.util.Settings
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by hongya on 14/04/2018.
  * ${Main}
  */
object DataEtl {

  def main(args: Array[String]): Unit = {

    //初始化
    println("初始化...")
    // 初始化spark, 设置spark参数
    val conf = new SparkConf().setAppName(Settings.get("app.name","app_coupon"))

    if (Settings.get("env") == "dev") {
      conf.setMaster("local[2]")
      conf.set("spark.testing.memory", "2048000000")
    }

    val sc = new SparkContext(conf)

    val input = sc.textFile(Settings.get("path_to_offline")).map{
      line =>
        line.split(",")
    }.filter{
      seq => seq.length == 7
    }.map{ seq =>
      Row(seq(0),seq(1),seq(2),seq(3),seq(4),seq(5),seq(6))
    }


    val sqlContext = new SQLContext(sc)

    // user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date
    val data_schema = new StructType()
      .add(StructField("user_id", StringType))
      .add(StructField("merchant_id", StringType))
      .add(StructField("coupon_id", StringType))
      .add(StructField("discount_rate", StringType))
      .add(StructField("distance", StringType))
      .add(StructField("date_received", StringType))
      .add(StructField("date", StringType))

    // 从数据取出不为空的数据作为ETL后的数据
    val data = sqlContext.createDataFrame(input,data_schema).filter{ row =>
      row.getAs[String]("coupon_id") != "null"
    }

    // 没有消费日期，说明没有消费
    val test1 = data.filter(row => row.getAs[String]("date") == "null")
    test1.take(10).foreach(println)
    println("有优惠券的数据中，没有消费的条数为：" + test1.count())
    test1.repartition(1).write.csv(Settings.get("path_to_offline_train_N"))

    // 有消费日期，说明消费了
    val test2 = data.filter(row => row.getAs[String]("date") != "null")

    test2.take(10).foreach(println)
    println("有优惠券的数据中，并且消费的条数为：" + test2.count())
    test2.repartition(1).write.csv(Settings.get("path_to_offline_train_P"))

  }

}
