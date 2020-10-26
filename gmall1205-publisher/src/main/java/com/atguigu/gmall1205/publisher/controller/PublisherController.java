package com.atguigu.gmall1205.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1205.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import java.time.LocalDate;
import java.util.*;

@RestController
public class PublisherController {

    @Autowired
    PublisherService publisherService;

    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {
        //想得到json数组，可以在代码中创建java的map的list，然后用fastjson转换
        List<Map<String, String>> totalList = new ArrayList<>();
        HashMap<String, String> dauMap = new HashMap();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");

        dauMap.put("value", publisherService.getDau(date).toString());
        totalList.add(dauMap);

        HashMap<String, String> newMidMap = new HashMap();
        newMidMap.put("id", "new_mid");
        newMidMap.put("name", "新增设备");
        newMidMap.put("value", "233");
        totalList.add(newMidMap);

        HashMap<String, String> amountMap = new HashMap();
        amountMap.put("id", "order_amount");
        amountMap.put("name", "新增交易额");
        amountMap.put("value", publisherService.getTotalAmount(date).toString());
        totalList.add(amountMap);

        return JSON.toJSONString(totalList);
    }


    @GetMapping("realtime-hour")
    public String getRealtimeHour(@RequestParam("id") String id, @RequestParam("date") String date){
        if("dau".equals(id)){
            Map todayDauHourMap = publisherService.getHourDau(date);
            Map yesterdayDauHourMap = publisherService.getHourDau(date2Yesterday(date));

            Map<String, Map> hourMap = new HashMap<>();
            hourMap.put("today", todayDauHourMap);
            hourMap.put("yesterday", yesterdayDauHourMap);
            return JSON.toJSONString(hourMap);
        } else if ("order_amount".equals(id)){
            Map todayAmoutHourMap = publisherService.getHourAmount(date);
            Map yesterdayAmountHourMap = publisherService.getHourAmount(date2Yesterday(date));

            Map<String, Map> hourMap = new HashMap<>();
            hourMap.put("today", todayAmoutHourMap);
            hourMap.put("yesterday", yesterdayAmountHourMap);
            return JSON.toJSONString(hourMap);
        }
        return null;
    }



    /**
     * 根据传入的日期转换成昨天
     *
     * @param date
     * @return
     */
    public String date2Yesterday(String date) {
        return LocalDate.parse(date).minusDays(1).toString();
    }

}

