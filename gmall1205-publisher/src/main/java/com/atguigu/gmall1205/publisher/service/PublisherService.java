package com.atguigu.gmall1205.publisher.service;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface PublisherService {


    public Long getDau(String date);
    public Map<String, Long> getHourDau(String date);
    public Double getTotalAmount(String date);
    public Map<String,Double> getHourAmount(String date);

    public Map<String, Object> getSaleDetailAndAggregationByField(String date,
                                                                  String keyword,
                                                                  String field,
                                                                  int size,
                                                                  int page,
                                                                  int pageSize) throws IOException;

 }
