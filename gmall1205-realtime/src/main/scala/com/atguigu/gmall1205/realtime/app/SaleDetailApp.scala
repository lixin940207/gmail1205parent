package com.atguigu.gmall1205.realtime.app

import java.util.Properties

import com.alibaba.fastjson.JSON
import com.atguigu.gmall1205.common.constant.GmallConstant
import com.atguigu.gmall1205.realtime.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.gmall1205.realtime.util.{EsUtil, MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions.asScalaSet

object SaleDetailApp {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("dau_app").setMaster("local[*]")
        val ssc = new StreamingContext(sparkConf, Seconds(5))

        //读取order-detail 和order_info
        val orderInfoStream = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.ORDER_TOPIC).map{
            case (_, jsonString) =>
                val orderInfo = JSON.parseObject(jsonString, classOf[OrderInfo])
                (orderInfo.id, orderInfo)
        }

        val orderDetailStream = MyKafkaUtil.getKafkaStream(ssc, GmallConstant.DETAIL_TOPIC).map {
            case (_, jsonString) =>
                val orderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                (orderDetail.order_id, orderDetail)
        }

        //join流，因为两边都有可能确实数据，所以用outer join
        val jointStream: DStream[SaleDetail] = orderInfoStream.fullOuterJoin(orderDetailStream).mapPartitions{ it => {
            val client = RedisUtil.getJedisClient
            val partitionResult:Iterator[SaleDetail] = it.flatMap {
                //把orderInfo的数据缓存到redis中，因为orderInfo和orderDetail是一对多的关系
                case (orderId, (Some(orderInfo), Some(orderDetail))) =>
                    cacheOrderInfo(client, orderInfo)
                    //去OrderDetail缓存中，读出与当前orderInfo对应的OrderDetail
                    val orderDetailJsonSet = client.keys(s"order_detail_${orderInfo.id}_*")
                    val saleDetailSet = orderDetailJsonSet.map(jsonString => {
                        val orderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                    })
                    saleDetailSet + SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                case (orderId, (Some(orderInfo), None)) =>
                    cacheOrderInfo(client, orderInfo)
                    //去OrderDetail缓存中，读出与当前orderInfo对应的OrderDetail
                    val orderDetailJsonSet = client.keys(s"order_detail_${orderInfo.id}")
                    val saleDetailSet = orderDetailJsonSet.map(jsonString => {
                        val orderDetail = JSON.parseObject(jsonString, classOf[OrderDetail])
                        SaleDetail().mergeOrderInfo(orderInfo).mergeOrderDetail(orderDetail)
                    })
                    saleDetailSet
                case (orderId, (None, Some(orderDetail))) =>
                    // 去orderInfo缓存中读取数据，如果读到，则orderDetail不需要缓存，没读到才要缓存
                    val orderInfoString = client.get(s"order_info_${orderDetail.order_id}")
                    val orderInfo = JSON.parseObject(orderInfoString, classOf[OrderInfo])
                    if (orderInfoString != null && orderInfoString.startsWith("{")) {
                        SaleDetail().mergeOrderDetail(orderDetail).mergeOrderInfo(orderInfo) :: Nil
                    } else {
                        cacheOrderDetail(client, orderDetail)
                        Array[SaleDetail]()
                    }

            }
            //为什么用flatMap,
            // 因为(orderId, (Some(orderInfo), None))中会有对应多个saleDetail
            client.close()
            partitionResult
        }}

        //反查mysql，补齐userinfo
        val spark: SparkSession = SparkSession.builder().config(ssc.sparkContext.getConf).getOrCreate()
        import spark.implicits._
        val props: Properties = new Properties()
        props.setProperty("user", "root")
        props.setProperty("password", "123456")
        val finalStream =  jointStream.transform(rdd => {
            val userInfoRDD = spark.read.jdbc("jdbc:mysql://hadoop102:3306/gmall", "user_info", props)
                    .as[UserInfo]
                    .rdd
                    .map(userInfo => (userInfo.id, userInfo))

            val joinedRDD = rdd.map(saleDetail => (saleDetail.user_id, saleDetail)).join(userInfoRDD)

            // 把 userInfo 的信息补齐
            joinedRDD.map {
                case (_, (saleDetail, userInfo)) => saleDetail.mergeUserInfo(userInfo)
            }
        })

        finalStream.foreachRDD(rdd =>{
            EsUtil.insertBulk(GmallConstant.SALE_DETAIL_INDEX, rdd.collect())
        })

        ssc.start()
        ssc.awaitTermination()
    }

    def cacheOrderInfo(jedis: Jedis, orderInfo: OrderInfo) = {
        val orderInfoKey = s"order_info_${orderInfo.id}"
        cacheToRedis(jedis, orderInfoKey, orderInfo)
    }

    def cacheOrderDetail(jedis: Jedis, orderDetail: OrderDetail) = {
        val orderDetailKey = s"order_detail_${orderDetail.id}"
        cacheToRedis(jedis, orderDetailKey, orderDetail)

    }

    def cacheToRedis[T <: AnyRef](client: Jedis, key: String, value: T): Unit = {
        val jsonString: String = Serialization.write(value)(DefaultFormats)
        client.setex(key, 60 * 30, jsonString)//加超时时间
    }

}
