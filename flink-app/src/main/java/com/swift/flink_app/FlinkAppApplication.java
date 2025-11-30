package com.swift.flink_app;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.env.Environment;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.scheduling.annotation.EnableScheduling;


@SpringBootApplication
@EnableScheduling
@EnableKafka
public class FlinkAppApplication {

	@Autowired
	private Environment env;

	public static void main(String[] args) {
		var context = SpringApplication.run(FlinkAppApplication.class, args);
		Environment env = context.getEnvironment();
		System.out.println("Kafka bootstrap servers: " + env.getProperty("spring.kafka.bootstrap-servers"));
	}
}
