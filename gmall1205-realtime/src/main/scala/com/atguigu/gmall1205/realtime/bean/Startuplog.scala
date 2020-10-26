package com.atguigu.gmall1205.realtime.bean

import java.text.SimpleDateFormat
import java.util.Date

case class Startuplog(mid:String,
                      uid:String,
                      appId:String,
                      area:String,
                      os:String,
                      channel:String,
                      logType:String,
                      version:String,
                      var logDate:String = null,
                      var logHour:String = null,
                      var ts:Long
                     ) {
  private val f1 = new SimpleDateFormat("yyyy-MM-dd")
  private val f2 = new SimpleDateFormat("HH")
  logDate = f1.format(new Date(ts))
  logHour = f2.format(new Date(ts))
}
