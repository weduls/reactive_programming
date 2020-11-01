package com.wedul.reactivetest;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * 참고 https://tech.kakao.com/2018/05/29/reactor-programming/
 * https://javacan.tistory.com/entry/Reactor-Start-6-Thread-Scheduling
 */
class ReactorStreamTest {

    final List<String> basket1 = Arrays.asList(new String[]{"kiwi", "orange", "lemon", "orange", "lemon", "kiwi"});
    final List<String> basket2 = Arrays.asList(new String[]{"banana", "lemon", "lemon", "kiwi"});
    final List<String> basket3 = Arrays.asList(new String[]{"strawberry", "orange", "lemon", "grape", "strawberry"});
    final List<List<String>> baskets = Arrays.asList(basket1, basket2, basket3);
    final Flux<List<String>> basketFlux = Flux.fromIterable(baskets);

    final FruitInfo expected1 = new FruitInfo(
        Arrays.asList("kiwi", "orange", "lemon"),
        new LinkedHashMap<String, Long>() { {
            put("kiwi", 2L);
            put("orange", 2L);
            put("lemon", 2L);
        }}
    );
    final FruitInfo expected2 = new FruitInfo(
        Arrays.asList("banana", "lemon", "kiwi"),
        new LinkedHashMap<String, Long>() { {
            put("banana", 1L);
            put("lemon", 2L);
            put("kiwi", 1L);
        }}
    );
    final FruitInfo expected3 = new FruitInfo(
        Arrays.asList("strawberry", "orange", "lemon", "grape"),
        new LinkedHashMap<String, Long>() { {
            put("strawberry", 2L);
            put("orange", 1L);
            put("lemon", 1L);
            put("grape", 1L);
        }}
    );

    @Test
    @DisplayName("과일 종류를 중복없이 종류별 개수 나누기")
    void split_type_with_non_duplicate() {
        // concatMap은 순차적으로 Publisher 스트림의 값을 처리하고
        // flatMapSequential은 비동기적으로 동시성을 지원하면서 처리 (비동기 환경에서 사용하기 좋음)
        Flux<FruitInfo> fruitInfos = basketFlux.concatMap(basket -> {
            final Mono<List<String>> distinctFruits = Flux.fromIterable(basket).distinct().collectList();
            final Mono<Map<String, Long>> countFuitsMono = Flux.fromIterable(basket)
                    .groupBy(fruit -> fruit)
                    .concatMap(groupedFlux -> groupedFlux.count()
                        .map(count -> {
                            final Map<String, Long> fruitCount = new LinkedHashMap<>();
                            fruitCount.put(groupedFlux.key(), count);
                            return fruitCount;
                        })
                    )
                    .reduce((accumulatedMap, currentMap) -> new LinkedHashMap<String, Long>() {{
                        putAll(accumulatedMap);
                        putAll(currentMap);
                    }});
            return Mono.zip(distinctFruits, countFuitsMono, (distinct, count) -> new FruitInfo(distinct, count));
        });

        fruitInfos.subscribe(System.out::println);

        StepVerifier.create(fruitInfos)
            .expectNext(expected1)
            .expectNext(expected2)
            .expectNext(expected3)
            .verifyComplete();
    }

    @Test
    @DisplayName("과일 종류를 중복없이 종류별 개수 나누기(병렬)")
    void split_type_with_non_duplicate_with_pararell() throws InterruptedException {
        // distinct fruit와 map의 효율성을 위해 비동기로 실행 시키나
        // 메인 스레드가 죽었을 때 distinct를 구동하는 스레드가 정지되는 것을 막기위해 대기하는 countdownlatch 사용
        CountDownLatch countDownLatch = new CountDownLatch(1);

        // concatMap은 순차적으로 Publisher 스트림의 값을 처리하고
        // flatMapSequential은 비동기적으로 동시성을 지원하면서 처리 (비동기 환경에서 사용하기 좋음)
        Flux<FruitInfo> fruitInfos = basketFlux.concatMap(basket -> {
            final Mono<List<String>> distinctFruits = Flux.fromIterable(basket).log().distinct().collectList().subscribeOn(Schedulers.parallel());;
            final Mono<Map<String, Long>> countFuitsMono = Flux.fromIterable(basket).log()
                    .groupBy(fruit -> fruit)
                    .concatMap(groupedFlux -> groupedFlux.count()
                        .map(count -> {
                            final Map<String, Long> fruitCount = new LinkedHashMap<>();
                            fruitCount.put(groupedFlux.key(), count);
                            return fruitCount;
                        })
                    )
                    .reduce((accumulatedMap, currentMap) -> new LinkedHashMap<String, Long>() {{
                        putAll(accumulatedMap);
                        putAll(currentMap);
                    }})
                    .subscribeOn(Schedulers.parallel());
            return Mono.zip(distinctFruits, countFuitsMono, (distinct, count) -> new FruitInfo(distinct, count));
        });

        fruitInfos.subscribe(System.out::println,
            error -> {
                System.err.println(error);
                countDownLatch.countDown();
            }, // 에러 발생시 출력하고 countDown, onError(Throwable)
            () -> {
                System.out.println("complete");
                countDownLatch.countDown();
            });

        countDownLatch.await(2, TimeUnit.SECONDS);

        StepVerifier.create(fruitInfos)
            .expectNext(expected1)
            .expectNext(expected2)
            .expectNext(expected3)
            .verifyComplete();
    }

    /**
     * publishOn은 next, complete, error 신호에 대해 별도 쓰레도로 처리가 가능
     * subscribeOn은 subscriber가 시퀀스에 대한 request 신호를 별도 스케줄러로 실행한다. (첫번째 publishOn 신호까지의 작업을 수행한다..)
     * subscribeOn()이 publishOn() 뒤에 위치하면 실질적으로 prefetch할 때를 제외하면 적용되지 않는다. subscribeOn()은 원본 시퀀스의 신호 발생을 처리할 스케줄러를 지정하므로 시퀀스 생성 바로 뒤에 subscribeOn()을 지정하도록 하자. 또한 두 개 이상 subscribeOn()을 지정해도 첫 번째 subscribeOn()만 적용된다.
     * @throws InterruptedException
     */
    @Test
    @DisplayName("과일 종류를 중복없이 종류별 개수 나누기(연산지점 병렬 - source만 유일 스레드) - hot 방식 사용")
    void split_type_with_non_duplicate_with_pararell_hot() throws InterruptedException {
        // distinct fruit와 map의 효율성을 위해 비동기로 실행 시키나
        // 메인 스레드가 죽었을 때 distinct를 구동하는 스레드가 정지되는 것을 막기위해 대기하는 countdownlatch 사용
        CountDownLatch countDownLatch = new CountDownLatch(1);

        // concatMap은 순차적으로 Publisher 스트림의 값을 처리하고
        // flatMapSequential은 비동기적으로 동시성을 지원하면서 처리 (비동기 환경에서 사용하기 좋음)
        Flux<FruitInfo> fruitInfos = basketFlux.concatMap(basket -> {
            //  최소 2개의 구독자가 구독을 하면 자동으로 구독하는 Flux 반환
            final Flux<String> source = Flux.fromIterable(basket).publish().autoConnect(2).publishOn(Schedulers.single());
            final Mono<List<String>> distinctFruits = source.publishOn(Schedulers.parallel()).distinct().collectList().log();;
            final Mono<Map<String, Long>> countFuitsMono = source.publishOn(Schedulers.parallel())
                .groupBy(fruit -> fruit)
                .concatMap(groupedFlux -> groupedFlux.count()
                    .map(count -> {
                        final Map<String, Long> fruitCount = new LinkedHashMap<>();
                        fruitCount.put(groupedFlux.key(), count);
                        return fruitCount;
                    })
                )
                .reduce((accumulatedMap, currentMap) -> new LinkedHashMap<String, Long>() {{
                    putAll(accumulatedMap);
                    putAll(currentMap);
                }})
                .log();
            return Mono.zip(distinctFruits, countFuitsMono, (distinct, count) -> new FruitInfo(distinct, count));
        });

        fruitInfos.subscribe(System.out::println,
            error -> {
                System.err.println(error);
                countDownLatch.countDown();
            }, // 에러 발생시 출력하고 countDown, onError(Throwable)
            () -> {
                System.out.println("complete");
                countDownLatch.countDown();
            });

        countDownLatch.await(2, TimeUnit.SECONDS);

        StepVerifier.create(fruitInfos)
            .expectNext(expected1)
            .expectNext(expected2)
            .expectNext(expected3)
            .verifyComplete();
    }

}
