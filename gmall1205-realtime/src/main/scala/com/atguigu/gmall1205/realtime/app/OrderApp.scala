package com.atguigu.gmall1205.realtime.app

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1205.common.constant.GmallConstant
import com.atguigu.gmall1205.realtime.bean.OrderInfo
import com.atguigu.gmall1205.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.phoenix.spark._

object OrderApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val ssc = new StreamingContext(conf, Seconds(2))
    val sourceDSteam: InputDStream[(String, String)] = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.ORDER_TOPIC)

    val orderInfoStream = sourceDSteam.map {
      case (_, jsonString) =>
        val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
        orderInfo
    }

    orderInfoStream.foreachRDD(rdd => {
      rdd.saveToPhoenix(
        "GMALL_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        zkUrl = Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    ssc.start()
    ssc.awaitTermination()

  }
}

