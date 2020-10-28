package com.atguigu.gmall1205.realtime.util

import redis.clients.jedis.Jedis


object RedisUtil {

  //var jedisPool:JedisPool=null

    val config = PropertiesUtil.load("config.properties")
    val host = config.getProperty("redis.host")
    val port = config.getProperty("redis.port")
  def getJedisClient: Jedis = {
//    if(jedisPool==null){
//      //      println("开辟一个连接池")
//      val config = PropertiesUtil.load("config.properties")
//      val host = config.getProperty("redis.host")
//      val port = config.getProperty("redis.port")
      /*
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(40)   //最大空闲
      jedisPoolConfig.setMinIdle(10)     //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true)  //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(1000*60)//忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
      jedisPoolConfig.setTestOnReturn(true)

      jedisPool=new JedisPool(jedisPoolConfig,host,port.toInt, 1000*60)

       */
    // }
    //    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
    //   println("获得一个连接")
    //jedisPool.getResource


      val jedis: Jedis = new Jedis(host, port.toInt, 1000 * 60)
      jedis.connect()
      jedis
  }
}

