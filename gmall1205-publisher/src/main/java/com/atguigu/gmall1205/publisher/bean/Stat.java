package com.atguigu.gmall1205.publisher.bean;

import java.util.ArrayList;
import java.util.List;

public class Stat {

    String title;
    List<Option> options = new ArrayList<>();

    public Stat() {

    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Option> getOptions() {
        return options;
    }

    public void setOptions(List<Option> options) {
        this.options = options;
    }

    public void addOption(Option option){
        this.options.add(option);
    }
}
