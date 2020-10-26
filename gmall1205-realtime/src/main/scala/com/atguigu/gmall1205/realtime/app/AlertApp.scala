package com.atguigu.gmall1205.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1205.common.constant.GmallConstant
import com.atguigu.gmall1205.realtime.bean.EventLog
import com.atguigu.gmall1205.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

object AlertApp {
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setAppName("alert_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))
    //1.从kafka读取数据并添加窗口：窗口长度5m，步长5s
    val rawDStream = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.KAFKA_TOPIC_EVENT).window(Minutes(5))

    //2。封装数据
    val eventLogStream = rawDStream.map {
      case (_, jsonObject) =>
        JSON.parseObject(jsonObject, classOf[EventLog])
    }
    eventLogStream.print(1000)

    ssc.start()
    ssc.awaitTermination()

  }
}
