package com.wedul.rxjavatemperature.service;

import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.wedul.rxjavatemperature.dto.Temperature;
import rx.Subscriber;

/**
 *
 * reactive
 *
 * @author wedul
 * @version
 * @since 2019/12/08
 **/
public class RxSseEmitter extends SseEmitter {
	static final long SSE_SESSION_TIMEOUT = 30 * 60 * 1000L;
	private final Subscriber<Temperature> subscriber;

	/**
	 * 구독자로써 RxJavaTemperature가 발행한 데이터를 전달하는 구독자
	 */
	public RxSseEmitter() {
		super(SSE_SESSION_TIMEOUT);

		this.subscriber = new Subscriber<Temperature>() {
			@Override
			public void onCompleted() {
			}

			@Override
			public void onError(Throwable e) {
			}

			@Override
			public void onNext(Temperature temperature) {
				try {
					RxSseEmitter.this.send(temperature);
				} catch (Exception e) {
					unsubscribe();
				}
			}
		};

		onCompletion(subscriber::unsubscribe);
		onTimeout(subscriber::unsubscribe);
	}

	public Subscriber<Temperature> getSubscriber() {
		return subscriber;
	}

}
