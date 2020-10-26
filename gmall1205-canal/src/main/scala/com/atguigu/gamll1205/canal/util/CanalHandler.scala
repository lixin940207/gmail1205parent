package com.atguigu.gamll1205.canal.util

import java.util

import com.alibaba.fastjson.JSONObject
import com.alibaba.otter.canal.protocol.CanalEntry
import com.alibaba.otter.canal.protocol.CanalEntry.{EventType, RowData}
import com.atguigu.gmall1205.common.constant.GmallConstant



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
      // 1. rowData 表示一行数据, 通过他得到每一列. 首先遍历每一行数据
      for (rowData <- rowDataList) {
        val jsonObj = new JSONObject()
        // 2. 得到每行中, 所有列组成的列表
        val columnList: util.List[CanalEntry.Column] = rowData.getAfterColumnsList
        for (column <- columnList) {
          // 3. 得到列名和列值
          val key = column.getName
          val value = column.getValue
          jsonObj.put(key, value)
        }
        MyKafkaUtil.send(GmallConstant.ORDER_TOPIC, jsonObj.toString)
      }
    }
  }
}

