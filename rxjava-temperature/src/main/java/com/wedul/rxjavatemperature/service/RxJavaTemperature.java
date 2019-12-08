package com.wedul.rxjavatemperature.service;

import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Service;

import com.wedul.rxjavatemperature.dto.Temperature;
import rx.Observable;

/**
 *
 * temperature 모듈에서 했던 Applicationlistener 방식이 아니라 리액티브 스트림을 반환하는 서비스
 *
 * @author wedul
 * @version
 * @since 2019/12/08
 **/
@Service
public class RxJavaTemperature {

	private final Random rnd = new Random();

	/**
	 * 0 ~ integer Max Value까지 시퀀스를 만들면서
	 * 그 데이터를 5초동안 지연시키고 그 값을 온도 데이터로 변형한뒤
	 * 브로딩 캐스팅을 하고 이는 구독자가 있을 때만 진행하라는 Observerable을 추가 한것 (발행자)
	 */
	private final Observable<Temperature> dataStream =
		Observable
			.range(0, Integer.MAX_VALUE) // 정수 시퀀스 발
			.concatMap(tick -> Observable
				.just(tick)
				.delay(rnd.nextInt(5000), TimeUnit.MILLISECONDS)
				.map(tickValue -> this.probe()))
			.publish() // 브로딩 캐스팅
			.refCount(); // 구독자가 있을 때만 진행

	private Temperature probe() {
		return new Temperature(16 + rnd.nextGaussian() * 10);
	}

	public Observable<Temperature> temperatureStream() {
		return dataStream;
	}

}
