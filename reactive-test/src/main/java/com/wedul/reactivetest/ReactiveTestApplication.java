package com.wedul.reactivetest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import reactor.core.publisher.Flux;

@SpringBootApplication
public class ReactiveTestApplication {

	public static void main(String[] args) {
		SpringApplication.run(ReactiveTestApplication.class, args);
	}

}
