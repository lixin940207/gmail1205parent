package com.atguigu.gamll1205.canal.util

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowData}
import com.atguigu.gmall1205.common.constant.GmallConstant
import scala.collection.JavaConversions._



object CanalHandler {
  /**
   * 处理从 canal 取来的数据
   *
   * @param tableName   表名
   * @param eventType   事件类型
   * @param rowDataList 数据类别
   */
  def handle(tableName: String, eventType: EventType, rowDataList: util.List[RowData]) = {
    import scala.collection.JavaConversions._
    if ("order_info" == tableName && eventType == EventType.INSERT && rowDataList.size() > 0) {
      sendRowListToKafka(rowDataList, GmallConstant.ORDER_TOPIC)
    } else if ("order_detail" == tableName && eventType == EventType.INSERT && rowDataList != null && !rowDataList.isEmpty) {
      sendRowListToKafka(rowDataList, GmallConstant.DETAIL_TOPIC)
    }

  }
  private def sendRowListToKafka(rowDataList: util.List[CanalEntry.RowData], topic: String): Unit = {
    for (rowData <- rowDataList) {

      val jsonObj = new JSONObject()
      // 变化后的列
      val columnList: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
      for (column <- columnList) {
        jsonObj.put(column.getName, column.getValue)
      }
      println(jsonObj.toJSONString)
      // 写入到Kafka
      MyKafkaUtil.send(topic, jsonObj.toJSONString)
    }
  }

}

