package com.atguigu.gmall1205.realtime.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1205.common.constant.GmallConstant
import com.atguigu.gmall1205.realtime.bean.Startuplog
import com.atguigu.gmall1205.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._


object DauApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val rawDStream = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.KAFKA_TOPIC_STARTUP)

    val startuplogStream = rawDStream.map {
      case (_, v) => JSON.parseObject(v, classOf[Startuplog])
    }
    startuplogStream.print

    //去重
    //3.1 从redis中读取语句启动的记录
    val filteredStream = startuplogStream.transform(rdd => {
      //3.2 读取redis中已经启动的记录
      val client = RedisUtil.getJedisClient
      //topic_startup:2020-10-24
      val midSet = client.smembers(GmallConstant.KAFKA_TOPIC_STARTUP + ":" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
      client.close()
      val bd = ssc.sparkContext.broadcast(midSet)
      //3.3 过滤掉那些已经启动过的设备
      rdd.filter(log => {
        !bd.value.contains(log.mid)
      }).map(log => (log.mid, log)).groupByKey.map {
        case (mid, logIt) => logIt.toList.minBy(_.ts)
      }
    })

    //3.4 把第一次启动的设备的mid写入redis
    filteredStream.foreachRDD(rdd => {
      rdd.foreachPartition(logIt => {
        //获取redis连接
        val client = RedisUtil.getJedisClient
        //写mid到redis中
        logIt.foreach(log => {
          client.sadd(GmallConstant.KAFKA_TOPIC_STARTUP + ":" + log.logDate, log.mid)
        })
        client.close()
      })
    })

    filteredStream.print
    //4 写入hbase
    filteredStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL_DAU",
        Seq("MID", "UID", "APPID", "AREA", "OS", "CHANNEL", "LOGTYPE", "VERSION", "LOGDATE", "LOGHOUR", "TS"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181")
      )
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
