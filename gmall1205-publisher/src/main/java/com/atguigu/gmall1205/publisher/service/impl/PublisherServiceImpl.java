package com.atguigu.gmall1205.publisher.service.impl;

import com.atguigu.gmall1205.publisher.mapper.DauMapper;
import com.atguigu.gmall1205.publisher.mapper.OrderMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Service
public class PublisherServiceImpl implements com.atguigu.gmall1205.publisher.service.PublisherService {

    @Autowired
    DauMapper dauMapper;

    @Override
    public Long getDau(String date) {
        return dauMapper.getDau(date);
    }

    @Override
    public Map<String, Long> getHourDau(String date) {
        List<Map> mapList = dauMapper.getHourDau(date);
        HashMap<String, Long> result = new HashMap<>();
        for (Map map : mapList) {
            String loghour = (String) map.get("LOGHOUR");
            Long count = (Long) map.get("COUNT");
            result.put(loghour, count);
        }
        return result;
    }

    @Autowired
    OrderMapper orderMapper;

    @Override
    public Double getTotalAmount(String date) {
        Double total = orderMapper.getTotalAmount(date);
        return total == null ? 0 : total;
    }

    @Override
    public Map<String,Double> getHourAmount(String date) {
        List<Map> mapList = orderMapper.getHourAmount(date);
        HashMap<String, Double> result = new HashMap<>();
        for (Map map : mapList) {
            String hour = (String) map.get("CREATE_HOUR");
            Double sum = ((BigDecimal) map.get("SUM")).doubleValue();
            result.put(hour, sum);
        }
        return result;
    }
}