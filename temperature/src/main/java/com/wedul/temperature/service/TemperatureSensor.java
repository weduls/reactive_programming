package com.wedul.temperature.service;

import static java.util.concurrent.TimeUnit.*;

import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.PostConstruct;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import com.wedul.temperature.dto.Temperature;

/**
 *
 * reactive
 *
 * @author wedul
 * @version
 * @since 2019-12-07
 **/
@Service
public class TemperatureSensor {

	private final ApplicationEventPublisher publisher;
	private final Random random = new Random();
	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	public TemperatureSensor(ApplicationEventPublisher applicationEventPublisher) {
		this.publisher = applicationEventPublisher;
	}

	@PostConstruct
	public void startProcessing() {
		this.executor.schedule(this::probe, 1, SECONDS);
	}

	private void probe() {
		double temperature = 16 + random.nextGaussian() * 10;
		publisher.publishEvent(new Temperature(temperature));

		executor.schedule(this::probe, random.nextInt(5000), MILLISECONDS);
	}

}
