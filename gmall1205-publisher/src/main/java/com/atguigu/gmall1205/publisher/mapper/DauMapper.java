package com.atguigu.gmall1205.publisher.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {
    //方法内部到底执行什么样的sql,需要去写xml。在xml中定义sql
    Long getDau(String date);

    List<Map> getHourDau(String date);
}
