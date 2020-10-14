package com.atguigu.gmall1205.logger.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController //= controller+responsebody
public class LoggerController {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(LoggerController.class) ;
    @PostMapping("/log")
    public String doLog(@RequestParam("log") String logJson){

        // 补时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts",System.currentTimeMillis());
        // 落盘到logfile   log4j
        logger.info(jsonObject.toJSONString());
        return "success";
    }
}
