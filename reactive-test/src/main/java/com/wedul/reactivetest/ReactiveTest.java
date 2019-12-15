package com.wedul.reactivetest;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SynchronousSink;

import java.time.Instant;
import java.util.Random;
import java.util.function.Consumer;

@Slf4j
@Service
public class ReactiveTest implements InitializingBean {

    public void test() {
        subscribe();
        t1T2();
        predicate();
        threeItemGenerateSub();
        generateTwoParameter();
    }

    // generate 2 parameter
    private void generateTwoParameter() {
        // 첫번째 인자는 최초 상태,
        Flux<String> flux = Flux.generate(
            () -> {
                // 초기 데이터 (다음인자의 state로 넘어감)
                return 1;
            },
            // 상태와 sink
            (state, sink) -> {
                // 데이터 발생 신호
                sink.next("3 x " + state + "=" + 3 * state);
                if (state == 10) {
                    sink.complete();
                }
                return state + 1;
            });
        flux.subscribe(System.out::print);
    }

    private void subscribe() {
        Flux.range(0, 21)
            .doOnNext(System.out::print)
            .subscribe(new Subscriber<Integer>() {
                private Subscription subscription;

                @Override
                public void onSubscribe(Subscription s) {
                    this.subscription = s;
                    log.info("Subscriber.onSubscribe");
                    subscription.request(100);
                }

                @Override
                public void onNext(Integer integer) {
                    log.info("Subscriber.onNext: " + integer);
                    subscription.request(1);
                }

                @Override
                public void onError(Throwable t) {
                    log.info("Subscriber.onError: " + t.getMessage());
                }

                @Override
                public void onComplete() {
                    log.info("Subscriber.onComplete");
                }
            });
    }

    private void t1T2() {
        // t1은 첫번째 요소 t2는 두번째 요소
        Flux.range(2018, 5)
            .timestamp()
            .index()
            .subscribe(e -> log.info("index: {}, ts: {}, value: {}", e.getT1(), Instant.ofEpochMilli(e.getT2().getT1()), e.getT2().getT2()));
    }

    private void predicate() {
        Flux<Integer> data = Flux.just(3, 5, 7, 9, 11, 15, 16, 17);

        // any 검증
        data.any(e -> e % 2 == 0)
            .subscribe(hasEvents -> log.info("Has any evens: {}", hasEvents));

        // all 검증
        data.all(e -> e % 2 == 0)
            .subscribe(hasEvents -> log.info("Has all evens: {}", hasEvents));
    }

    private void threeItemGenerateSub() {
        // 3개씩 만들고
        Flux<Integer> seq = Flux.generate(new Consumer<SynchronousSink<Integer>>() {
            private int emitCount = 0;
            private Random rand = new Random();

            @Override
            public void accept(SynchronousSink<Integer> sink) {
                emitCount++;

                int data = rand.nextInt(100) + 1;
                log.info("Generator sink next " + data);
                sink.next(data);

                if (emitCount == 10) {
                    log.info("Generator sink complete");
                    sink.complete();
                }
            }
        });

        // 3개씩 소비
        seq.subscribe(new BaseSubscriber<Integer>() {
            private int receiveCount = 0;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                log.info("Subscriber#onSubscribe");
                log.info("Subscriber request first 3 items");
                // 구독 시점 3개의 데이터를 요청
                request(3);
            }

            @Override
            protected void hookOnComplete() {
                log.info("Subscriber#onComplete");
            }

            @Override
            protected void hookOnNext(Integer value) {
                // 다시 3개의 데이터를 요청
                log.info("Subscribe#onNext : " + value);
                receiveCount++;
                if (receiveCount % 3 == 0) {
                    log.info("Subscriber request next 3items");
                    request(3);
                }
            }
        });
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        test();
    }
}
