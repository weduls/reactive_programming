package com.wedul.reactivetest;

import java.time.Instant;

import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
@Service
public class ReactiveTest implements InitializingBean {

	public void test() {
		// t1은 첫번째 요소 t2는 두번째 요소
		Flux.range(2018, 5)
			.timestamp()
			.index()
			.subscribe(e -> log.info("index: {}, ts: {}, value: {}", e.getT1(), Instant.ofEpochMilli(e.getT2().getT1()), e.getT2().getT2()));

		Flux<Integer> data = Flux.just(3, 5, 7, 9, 11, 15, 16, 17);

		// any 검증
		data.any(e -> e % 2 == 0)
			.subscribe(hasEvents -> log.info("Has any evens: {}", hasEvents));

		// all 검증
		data.all(e -> e % 2 == 0)
			.subscribe(hasEvents -> log.info("Has all evens: {}", hasEvents));

	}

	@Override
	public void afterPropertiesSet() throws Exception {
		test();
	}
}
