package com.atguigu.gmall1205.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1205.common.constant.GmallConstant
import com.atguigu.gmall1205.realtime.bean.{AlertInfo, EventLog}
import com.atguigu.gmall1205.realtime.util.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks.{break, breakable}

object AlertApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("alert_app").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        //1.从kafka读取数据并添加窗口：窗口长度5m，步长5s
        val rawDStream = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.KAFKA_TOPIC_EVENT).window(Minutes(5))

        //2。封装数据
        val eventLogStream = rawDStream.map {
            case (_, jsonObject) =>
                val log = JSON.parseObject(jsonObject, classOf[EventLog])
                (log.mid, log)
        }
        //按照mid分组
        val groupedEventLogDStream = eventLogStream.groupByKey

        // 4. 预警的业务逻辑
        val checkCouponAlertDStream: DStream[(Boolean, AlertInfo)] = groupedEventLogDStream.map {
            case (mid, logIt) => {
                //logIt指的是在5分钟内，这个mid上的所有事件日志

                //必须要用java的set，scala的set es不认识
                val uids: util.HashSet[String] = new util.HashSet[String]() //用户
                val itemIds: util.HashSet[String] = new util.HashSet[String]() //优惠券对应的商品商品
                val eventIds: util.ArrayList[String] = new util.ArrayList[String]() //所有事件

                var isBrowserProduct: Boolean = false // 是否浏览商品, 默认没有浏览
                // 1. 遍历这个设备上5分钟内的所有事件日志
                breakable {
                    logIt.foreach(log => {
                        eventIds.add(log.eventId)
                        // 2. 记录下领优惠全的所有用户
                        if (log.eventId == "coupon") {
                            uids.add(log.uid) // 领优惠券的用户id
                            itemIds.add(log.itemId) // 用户领券的商品id
                        } else if (log.eventId == "clickItem") { // 如果有浏览商品
                            isBrowserProduct = true
                            break
                        }
                    })
                }
                //2. 组合成元组  (是否预警, 预警信息)
                (!isBrowserProduct && uids.size() >= 3, AlertInfo(mid, uids, itemIds, eventIds, System.currentTimeMillis()))
            }
        }
        // 5. 过滤掉不需要报警的信息
        val filteredDStream: DStream[AlertInfo] = checkCouponAlertDStream.filter(_._1).map(_._2)


        ssc.start()
        ssc.awaitTermination()

    }
}
