package com.atguigu.gmall1205.publisher;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall1205.publisher.mapper")
public class Gmall1205PublisherApplication {

	public static void main(String[] args) {
		SpringApplication.run(Gmall1205PublisherApplication.class, args);
	}

}
