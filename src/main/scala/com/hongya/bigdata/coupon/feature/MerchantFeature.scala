package com.hongya.bigdata.coupon.feature

import com.hongya.bigdata.coupon.util.Settings
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by dengziming on 14/04/2018.
  * ${Main}
  */
object MerchantFeature {

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


    val OffTrain = sc.textFile(Settings.get("path_to_offline")).map{
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


    // 正例样本，保存为一个表格
    sqlContext.createDataFrame(OffTrain,data_schema).createOrReplaceTempView("train")

    // 现在train_P表格中的数据是所有的消费过得用户，我们通过一个sql得到我们需要的两个特征：
    // FUser1 线下领取优惠券后消费次数
    // FUser2 线下消费总次数

    val add_feature =
      """
        |select
        |    a.merchant_id,
        |    b.FMer1,
        |    c.FMer2,
        |    d.FMer3
        |from
        |    (select distinct merchant_id from train ) a
        |left join
        |    (select merchant_id,sum(1) as FMer1 from train where coupon_id<>'null' group by merchant_id) b
        |on
        |    a.merchant_id = b.merchant_id
        |left join
        |    (select merchant_id,sum(1) as FMer2 from train where coupon_id<>'null' and date<>'null' group by merchant_id) c
        |on
        |    a.merchant_id = c.merchant_id
        |left join
        |    (select merchant_id,sum(1) as FMer3 from train where date<>'null' group by merchant_id) d
        |on
        |    a.merchant_id = d.merchant_id
      """.stripMargin

    sqlContext.sql(add_feature).repartition(1).write.csv(Settings.get("path_to_FMer"))

  }
}
