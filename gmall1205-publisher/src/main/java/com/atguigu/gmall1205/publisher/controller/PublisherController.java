package com.atguigu.gmall1205.publisher.controller;

import com.alibaba.fastjson.JSON;
import com.atguigu.gmall1205.publisher.bean.Option;
import com.atguigu.gmall1205.publisher.bean.SaleDetailInfo;
import com.atguigu.gmall1205.publisher.bean.Stat;
import com.atguigu.gmall1205.publisher.service.PublisherService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
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


    /*
接口: http://localhost:8070/sale_detail?date=2019-05-20&&startpage=1&&size=5&&keyword=手机小米

 */
    @GetMapping("/sale_detail")
    public String saleDetail(@RequestParam("date") String date,
                   @RequestParam("startpage") int page,
                   @RequestParam("size") int pageSize,
                   @RequestParam("keyword") String keyword) throws IOException {
        Map<String, Object> resultAge = publisherService.getSaleDetailAndAggregationByField(date, keyword, "user_age", 100, page, pageSize);
        Map<String, Object> resultGender = publisherService.getSaleDetailAndAggregationByField(date, keyword, "user_gender", 2, page, pageSize);

        SaleDetailInfo saleDetailInfo = new SaleDetailInfo();
        Long total = (Long) resultAge.get("total");
        saleDetailInfo.setTotal(total);

        List<Map<String, Object>> detail = (List<Map<String, Object>>) resultAge.get("detail");
        saleDetailInfo.setDetail(detail);

        Stat genderStat = new Stat();
        genderStat.setTitle("用户性别占比");
        Map<String, Long> genderAgg = (Map<String, Long>)resultGender.get("agg");
        Set<Map.Entry<String, Long>> genderEntries = genderAgg.entrySet();
        for (Map.Entry<String, Long> genderEntry : genderEntries) {
            Option option = new Option();
            option.setName(genderEntry.getKey().replace("F", "女").replace("M", "男"));
            option.setValue(Double.valueOf(genderEntry.getValue()));
            genderStat.addOption(option);
        }
        saleDetailInfo.addStat(genderStat);

        //年龄的饼图
        Stat ageStat = new Stat();

        Map<String, Long> ageAgg = (Map<String, Long>) resultAge.get("agg");
        Set<Map.Entry<String, Long>> entries = ageAgg.entrySet();

        ageStat.addOption(new Option("20岁以下", 0.0));
        ageStat.addOption(new Option("20岁到30岁", 0.0));
        ageStat.addOption(new Option("30岁以上", 0.0));

        for (Map.Entry<String, Long> entry : entries) {
            int age = Integer.parseInt(entry.getKey());
            Long value = entry.getValue();
            if (age < 20){
                Option o0 = ageStat.getOptions().get(0);
                o0.setValue(o0.getValue() + value);
            } else if (age < 30){
                Option o1 = ageStat.getOptions().get(1);
                o1.setValue(o1.getValue() + value);
            } else{
                Option o2 = ageStat.getOptions().get(2);
                o2.setValue(o2.getValue() + value);
            }
        }

        saleDetailInfo.addStat(ageStat);

        return JSON.toJSONString(saleDetailInfo);


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

