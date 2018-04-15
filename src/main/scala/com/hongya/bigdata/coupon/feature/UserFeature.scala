package com.hongya.bigdata.coupon.feature

import com.hongya.bigdata.coupon.util.Settings
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  * Created by dengziming on 14/04/2018.
  * ${Main}
  */
object UserFeature {

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


    val OffTrainP = sc.textFile(Settings.get("path_to_offline_train_P") + "/*.csv").map{
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
    sqlContext.createDataFrame(OffTrainP,data_schema).createOrReplaceTempView("train_P")

    // 现在train_P表格中的数据是所有的消费过得用户，我们通过一个sql得到我们需要的两个特征：
    // FUser1 线下领取优惠券后消费次数
    // FUser2 线下消费总次数

    val add_feature =
      """
        |select
        |    a.user_id,
        |    b.FUser1,
        |    c.FUser2
        |from
        |    (select distinct user_id from train_P ) a
        |left join
        |    (select user_id,sum(1) as FUser1 from train_P group by user_id) b
        |on
        |    a.user_id = b.user_id
        |left join
        |    (select user_id,sum(1) as FUser2 from train_P where date<>'null' group by user_id) c
        |on
        |    a.user_id = c.user_id
      """.stripMargin

    sqlContext.sql(add_feature).repartition(1).write.csv(Settings.get("path_to_FUser"))

  }
}
