package com.atguigu.gmall1205.publisher.bean;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SaleDetailInfo {

    Long total;
    // 饼图集合
    List<Stat>  stats = new ArrayList<>();

    //明细
    List<Map<String, Object>> detail;

    public SaleDetailInfo() {

    }

    public Long getTotal() {
        return total;
    }

    public void setTotal(Long total) {
        this.total = total;
    }

    public List<Stat> getStats() {
        return stats;
    }

    public void setStats(List<Stat> stats) {
        this.stats = stats;
    }

    public void addStat(Stat stat){
        this.stats.add(stat);
    }

    public List<Map<String, Object>> getDetail() {
        return detail;
    }

    public void setDetail(List<Map<String, Object>> detail) {
        this.detail = detail;
    }
}
