package com.wedul.rxjavatemperature.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.wedul.rxjavatemperature.service.RxJavaTemperature;
import com.wedul.rxjavatemperature.service.RxSseEmitter;

/**
 *
 * reactive
 *
 * @author wedul
 * @version
 * @since 2019/12/08
 **/
@RestController
public class TemperatureController {

	private final RxJavaTemperature rxJavaTemperature;

	public TemperatureController(RxJavaTemperature rxJavaTemperature) {
		this.rxJavaTemperature = rxJavaTemperature;
	}

	/**
	 * temperature stream을 통해 데이터가 들어오면
	 * SseEmitter 구독자를 만들고 RxJavaTemperature에게 구독을 신청해서 데이터를 받아서
	 * UI에 전달한다.
	 * @return
	 */
	@GetMapping("/temperature-stream")
	public SseEmitter events() {
		RxSseEmitter rxSseEmitter = new RxSseEmitter();

		rxJavaTemperature.temperatureStream().subscribe(rxSseEmitter.getSubscriber());

		// ui에서는 결과로 받은 rxSseemitter를 이용하여 observable에서 전달한 데이터를 기반으로 화면에 보준다.
		return rxSseEmitter;
	}
}
