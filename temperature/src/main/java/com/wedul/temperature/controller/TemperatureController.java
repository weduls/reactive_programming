package com.wedul.temperature.controller;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

import org.springframework.context.event.EventListener;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.wedul.temperature.dto.Temperature;

/*
 이 방식의 문제점은 발행-구독 구조로 되어있기 때문에 고부하 및 고성능에 적합하지 않고 별도의 에러처리등등도 어려우며
 최종적으로 각 요청에 단순 값이 아닌 개별 스트림을 생성하여 부하를 구현해야하는것 자체가 문제.
 */
@RestController
public class TemperatureController {

	private final Set<SseEmitter> clients = new CopyOnWriteArraySet<>();

	/**
	 * Sse로 이벤트를 전달하는 역할만 진행
	 * @return
	 */
	@RequestMapping(value = "/temperature-system", method = RequestMethod.GET)
	public SseEmitter events() {
		SseEmitter sseEmitter = new SseEmitter();
		clients.add(sseEmitter);

		// event 타임아웃, 오류가 발생되거나 동작이 완료된경우 리스트에서 제거
		sseEmitter.onTimeout(() -> clients.remove(sseEmitter));
		sseEmitter.onCompletion(() -> clients.remove(sseEmitter));
		return sseEmitter;
	}

	@Async
	@EventListener
	public void handleMessage(Temperature temperature) {
		// request가 전달될 때 sseEmiter를 추가하고 terperatureSensor service에서 돌고 있는 service executor를 통해 데이터를 비동기로 클라이언트에게 전달

		List<SseEmitter> deadEmitters = new ArrayList<>();
		clients.forEach(sseEmitter -> {
			try {
				sseEmitter.send(temperature, MediaType.APPLICATION_JSON);
			} catch (Exception e) {
				deadEmitters.add(sseEmitter);
			}
		});

		clients.removeAll(deadEmitters);
	}

}
