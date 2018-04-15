package com.hongya.bigdata.coupon.model

import com.hongya.bigdata.coupon.util.Settings
import org.apache.spark.mllib.linalg.{DenseVector, Vector}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._

/**
  * Created by dengziming on 14/04/2018.
  * ${Main}
  */
object Model {

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

    val OffTrainN = sc.textFile(Settings.get("path_to_offline_train_N") + "/*.csv").map{
      line =>
        line.split(",")
    }.filter{
      seq => seq.length == 7
    }.map{ seq =>
      Row(seq(0),seq(1),seq(2),seq(3),seq(4),seq(5),seq(6))
    }

    val FUser = sc.textFile(Settings.get("path_to_FUser") + "/*.csv").map{
      line =>
        line.split(",").map{ token =>
          if ("" != token ) token else "0"
        }
    }.filter{
      seq => seq.length == 3
    }.map{ seq =>
      Row(seq(0),seq(1).toInt,seq(2).toInt)
    }

    val FMer = sc.textFile(Settings.get("path_to_FMer") + "/*.csv").map{
      line =>
        line.split(",").map{ token =>
          if ("" != token ) token else "0"
        }
    }.filter{
      seq => seq.length == 4
    }.map{ seq =>
      Row(seq(0),seq(1).toInt,seq(2).toInt,seq(3).toInt)
    }


    val testData = sc.textFile(Settings.get("path_to_test")).map{
      line =>
        line.split(",")
    }.filter{
      seq => seq.length == 6
    }.map{ seq =>
      Row(seq(0),seq(1),seq(2),seq(3),seq(4),seq(5),"")
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

    // user_id,FUser1,FUser2
    val fUser_schema = new StructType()
      .add(StructField("user_id", StringType))
      .add(StructField("FUser1", IntegerType))
      .add(StructField("FUser2", IntegerType))


    // merchant_id,FMer1,FMer2,FMer3
    val fMer_schema = new StructType()
      .add(StructField("merchant_id", StringType))
      .add(StructField("FMer1", IntegerType))
      .add(StructField("FMer2", IntegerType))
      .add(StructField("FMer3", IntegerType))

    val submit_schema = new StructType()
      .add(StructField("user_id", StringType))
      .add(StructField("coupon_id", StringType))
      .add(StructField("date_received", StringType))
      .add(StructField("prob", DoubleType))

    sqlContext.createDataFrame(OffTrainP,data_schema).take(10)
    sqlContext.createDataFrame(OffTrainN,data_schema).take(10)
    sqlContext.createDataFrame(FUser,fUser_schema).take(10)
    sqlContext.createDataFrame(FMer,fMer_schema).take(10)
    sqlContext.createDataFrame(testData,data_schema).take(10)
    // 正例样本，保存为一个表格
    sqlContext.createDataFrame(OffTrainP,data_schema).createOrReplaceTempView("train_P")
    // 负例样本
    sqlContext.createDataFrame(OffTrainN,data_schema).createOrReplaceTempView("train_N")
    // 特征
    sqlContext.createDataFrame(FUser,fUser_schema).createOrReplaceTempView("FUser")
    sqlContext.createDataFrame(FMer,fMer_schema).createOrReplaceTempView("FMer")
    // 提交的文件
    sqlContext.createDataFrame(testData,data_schema).createOrReplaceTempView("testData")


    // 将数据构造为训练数据的格式
    val train_sql =
      """
        |select
        |    a.user_id,
        |    a.merchant_id,
        |    coalesce(b.FUser1,0) as FUser1,
        |    coalesce(b.FUser2,0) as FUser2,
        |    coalesce(c.FMer1,0) as FMer1,
        |    coalesce(c.FMer2,0) as FMer2,
        |    coalesce(c.FMer3,0) as FMer3,
        |    a.label as label
        |from
        |    (
        |    select
        |        user_id ,
        |        merchant_id,
        |        1 as label
        |    from
        |        train_P
        |    union all
        |    select
        |        user_id ,
        |        merchant_id,
        |        0 as label
        |    from
        |        train_N
        |    ) a
        |left join
        |    FUser b
        |on
        |    a.user_id = b.user_id
        |left join
        |    FMer c
        |on
        |    a.merchant_id = c.merchant_id
        |
      """.stripMargin

    val test_sql =
      """
        |select
        |    a.user_id,
        |    a.merchant_id,
        |    a.coupon_id,
        |    a.date_received,
        |    coalesce(b.FUser1,0) as FUser1,
        |    coalesce(b.FUser2,0) as FUser2,
        |    coalesce(c.FMer1,0) as FMer1,
        |    coalesce(c.FMer2,0) as FMer2,
        |    coalesce(c.FMer3,0) as FMer3
        |from
        |    (
        |    select
        |        user_id ,
        |        merchant_id,
        |        coupon_id,
        |        date_received
        |    from
        |        testData
        |    ) a
        |left join
        |    FUser b
        |on
        |    a.user_id = b.user_id
        |left join
        |    FMer c
        |on
        |    a.merchant_id = c.merchant_id
      """.stripMargin

    sqlContext.sql(train_sql).take(10)

    val Array(trainingData,testingData) = dataFrame2LabelPoint(sqlContext.sql(train_sql)).randomSplit(Array(0.7,0.3))

    // 设置参数
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 3 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 4
    val maxBins = 32


    //训练模型
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    //进行预测
    val labelAndPreds = testingData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    labelAndPreds.take(10).foreach(println)
    // 计算误差
    val testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble / testData.count()
    println("Test Error = " + testErr)
    println("随机森林模型:\n" + model.toDebugString)
    //

    // 读取提交的数据进行预测后放入结果文件
    val submit_data = dataFrame2Features(sqlContext.sql(test_sql)).map{
      case (row,features) =>

        val label = model.predict(features)

        Row(row.get(0),
          row.get(1),
          row.get(2),
          label)
    }

    sqlContext.createDataFrame(submit_data,submit_schema)
      .repartition(1)
      .write
      .csv(Settings.get("path_to_Result"))

  }


  def dataFrame2LabelPoint(df : DataFrame): RDD[LabeledPoint] ={

    df.rdd.map{ row =>

      val FUser1 = row.getAs[Int]("FUser1").toDouble
      val FUser2 = row.getAs[Int]("FUser2").toDouble
      val FMer1 = row.getAs[Int]("FMer1").toDouble
      val FMer2 = row.getAs[Int]("FMer2").toDouble
      val FMer3 = row.getAs[Int]("FMer3").toDouble
      val label = row.getAs[Int]("label").toDouble

      val vector = new DenseVector(Array(FUser1,FUser2,FMer1,FMer2,FMer3))
      new LabeledPoint(label ,vector)
    }

  }

  def dataFrame2Features(df : DataFrame): RDD[(Row,Vector)] ={

    df.rdd.map{ row =>

      val user_id = row.getAs[String]("user_id")
      val coupon_id = row.getAs[String]("coupon_id")
      val date_received = row.getAs[String]("date_received")
      val FUser1 = row.getAs[Int]("FUser1").toDouble
      val FUser2 = row.getAs[Int]("FUser2").toDouble
      val FMer1 = row.getAs[Int]("FMer1").toDouble
      val FMer2 = row.getAs[Int]("FMer2").toDouble
      val FMer3 = row.getAs[Int]("FMer3").toDouble

      (Row(user_id,coupon_id,date_received),new DenseVector(Array(FUser1,FUser2,FMer1,FMer2,FMer3)))
    }

  }
}
