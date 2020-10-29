package com.atguigu.gmall1205.publisher.service.impl;

import com.atguigu.gmall1205.common.util.MyEsUtil;
import com.atguigu.gmall1205.publisher.mapper.DauMapper;
import com.atguigu.gmall1205.publisher.mapper.OrderMapper;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
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

    @Override
    public Map<String, Object> getSaleDetailAndAggregationByField(String date,
                                                                  String keyword,
                                                                  String field,
                                                                  int size,
                                                                  int page,
                                                                  int pageSize) throws IOException {
        JestClient client = MyEsUtil.getClient();
        String dls = MyEsUtil.getDSL(date, keyword, field, size, page, pageSize);
        Search search = new Search.Builder(dls).build();
        SearchResult searchResult = client.execute(search);
        HashMap<String, Object> result = new HashMap<>();
        //获取总数
        Long total = Long.valueOf(searchResult.getTotal());
        result.put("total", total);
        //获取聚合结果
        Map<String, Long> aggMap = new HashMap<>();
        List<TermsAggregation.Entry> buckets = searchResult.getAggregations().getTermsAggregation("groupby_" + field).getBuckets();
        for (TermsAggregation.Entry bucket : buckets) {
            String key = bucket.getKey();
            Long count = bucket.getCount();
            aggMap.put(key, count);
        }
        result.put("agg", aggMap);
        //获取详情
        List<Map<String, Object>> detailList = new ArrayList<>();
        List<SearchResult.Hit<HashMap, Void>> hits = searchResult.getHits(HashMap.class);
        for (SearchResult.Hit<HashMap, Void> hit : hits) {
            HashMap source = hit.source;
            detailList.add(source);
        }
        result.put("detail", detailList);
        return result;
    }
}