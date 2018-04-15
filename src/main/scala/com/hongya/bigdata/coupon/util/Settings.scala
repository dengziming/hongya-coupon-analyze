package com.hongya.bigdata.coupon.util

import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

/**
  * 项目配置信息
  * Created by  on 2017/1/11.
  */
object Settings {


  val logger: Logger = LoggerFactory.getLogger(Settings.getClass)
  val config: Config = ConfigFactory.load("config")

  val env: String = get("env")

  logger.info("###############################################")
  logger.info("############# 当前使用 " + env + " 配置 ################")
  logger.info("###############################################")

  logger.info(config.toString)


  def getConfig(path: String): Config = {
    config.getConfig(path)
  }

  def get(key: String): String = {
    config.getString(key)
  }

  def get(key: String,default:String): String = {
    try{
      val v = config.getString(key)
      if (v.isEmpty){
        default
      }else{
        v
      }
    }catch{
      case e: Exception => {
        logger.warn("get config err,usage default value : " + default)
        default
      }
    }
  }

  def getInt(key: String): Int = {
    config.getInt(key)
  }

  def getInt(key: String,default:Int): Int = {
    get(key,default + "").toInt
  }

  def getLong(key: String): Long = {
    config.getLong(key)
  }

  def getLong(key: String,default:Long): Long = {
    get(key,default + "").toLong
  }

  def getBoolean(key:String,default:Boolean): Boolean ={

    try{
      get(key).toLowerCase() match {

        case "false" => false
        case "true" => true
        case _ => default
      }
    }catch {case _:Exception => default}

  }



}
