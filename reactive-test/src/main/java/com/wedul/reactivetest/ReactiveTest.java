package com.wedul.reactivetest;

import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Service;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SynchronousSink;
import reactor.util.function.Tuple2;

import java.time.Instant;
import java.util.Random;
import java.util.function.Consumer;

@Slf4j
@Service
public class ReactiveTest implements InitializingBean {

    public void test() {
        // generate의 경우 subscribe가 있어야 발생하는 pull 방식
        subscribe();
        t1T2();
        predicate();
        threeItemGenerateSub();
        generateTwoParameter();

        // create (generate와 다른점은 generate의 경우 데이터를 한번에 하나만 보낼 수 있으나 create는 여러개 전달 가능)
//        createFlux();

        // 데이터 1대1 변환
        Flux<String> data = Flux.just("a", "bc", "def", "wxyz");
        fluxStrToInteger(data).subscribe(this::print);

        // 데이터 1대 n 변환
        fluxStrToFlatMap(data).subscribe(this::print);

        // filter 걸러내기
        fluxFilterLengthgteThree(data).subscribe(this::print);

        // merge (stream 발생 순서대로 merge)
        merge(data, Flux.range(1, 2)).subscribe(s -> log.info("merge data {}", s));

        // zip with (merge는 실행 속도로 묶는다면 시퀀스 하나 하나 순서대로 같이 매핑한다.), 합쳐지지 못한 스트림은 사라짐
        zipMapping(data, Flux.range(1, 2).map(d -> d.toString())).subscribe(s -> log.info("zip with data {}", s));

        // skip
        data.skipUntil(d -> d.length() >= 3).subscribe(s -> log.info("skip until {}", s));

        // 에러 처리
        makeError(data);

        // 에러 발생 시 대체값 사용
        whenErrorReplaceValue(data);

        // 에러 발생 시 값 대처
        replaceFlux(data);

        // 에러 발생 시 다른 에러로 대처
        replaceError(data);

        // retry when
        retryWhen(data);
    }

    private void retryWhen(Flux<String> data) {
        /*
             retry when는 Function<Flux<Throwable>, ? extends Publisher<?> whenFactory) 인자를 받는데
             Flux<Throwable>은 에러 exception에 해당된다.

             그래서 순서는 에러가 발생하면 Flux<Throwable>에 전달되고 Flux<Throwable>의 상태에 따라 에러를 전파하거나 재시도하거나 종료한다.
         */
        getCotainErrorMap(data)
            .retryWhen(
                // 2번에 에러 발생 또한 재시도를 할 경우에는 에러를 subscribe에게 리턴하지 않는다.
                err -> err.take(2)
            )
            .subscribe(System.out::print, e -> log.info("{}", e.getMessage()));

        // 2번 시도 후 에러 발생
        getCotainErrorMap(data)
            .retryWhen(errorsFlux -> errorsFlux.zipWith(Flux.range(1, 3),
                (error, index) -> {
                    if (index < 3) {
                        return index;
                    }
                    throw new RuntimeException("companion error");
                })
            ).subscribe(System.out::print, e -> log.info("{}", e.getMessage()));

        // retryWhen은 Flux<Throwable>에 따라서 에러를 발생시키기도 하고 아니기도 하다.
    }

    private void replaceError(Flux<String> data) {
        getCotainErrorMap(data)
            .onErrorMap(e -> new CustomException("다른 에러다!"))
            .subscribe(System.out::print, e -> log.info("{}", e.getMessage()));
    }

    private void makeError(Flux<String> data) {
        getCotainErrorMap(data).subscribe(System.out::print, i -> log.error("에러발생!!", i.getMessage()), () -> log.info("complete"));
    }

    private Flux<String> getCotainErrorMap(Flux<String> data) {
        return data.map(x -> {
            if (x.length() == 3) {
                throw new RuntimeException("에러가 발생했다.");
            }
            return x;
        });
    }

    private void replaceFlux(Flux<String> data) {
        getCotainErrorMap(data)
            .onErrorResume(error -> {
                if (error instanceof RuntimeException) {
                    return Flux.just("11");
                }
                return Flux.just("zz");
            })
            .subscribe(System.out::print, i -> log.error("에러발생!!", i.getMessage()), () -> log.info("complete"));
    }

    private void whenErrorReplaceValue(Flux<String> data) {
        getCotainErrorMap(data)
            .onErrorReturn("-2")
            .subscribe(System.out::print, i -> log.error("에러발생!!", i.getMessage()), () -> log.info("complete"));
    }

    private Flux<Tuple2<String, String>> zipMapping(Flux<String> data, Flux<String> data1) {
        return data.zipWith(data1);
    }

    private Flux<String> merge(Flux<String> data, Flux<Integer> subData) {
        return data.mergeWith(subData.map(z -> z.toString()));
    }

    private void print(Object s) {
        log.info("data content : {}", s);
    }

    private Flux<String> fluxFilterLengthgteThree(Flux<String> data) {
        return data.filter(str -> str.length() >= 3);
    }

    private Flux<Integer> fluxStrToFlatMap(Flux<String> data) {
        return data.flatMap(i -> Flux.range(1, i.length()));
    }

    private Flux<Integer> fluxStrToInteger(Flux<String> data) {
        /**
         * 1 -> Flux.range(1, 1) : [1] 생성
         * 2 -> Flux.range(1, 2) : [1, 2] 생성
         * 3 -> Flux.range(1, 3) : [1, 2, 3] 생성
         * 이런 식으로 매핑되어서 Flux<Flux<Integer>>로 보이지만 실제로 스트림이 Flux<Integer>로 스트림이 합쳐진다.
         */
        return data.map(t -> t.length());
    }

    private void createFlux() {
        Flux<Integer> flux = Flux.create((FluxSink<Integer> sink) -> {
            sink.onRequest(request -> {
                // request는 요청 건수
                for (int i = 0; i < request; i++) {
                    sink.next(i);
                }
            });
        });

        flux.subscribe(System.out::print);
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

    private class CustomException extends RuntimeException {
        public CustomException(String message) {
            super(message);
        }
    }
}
