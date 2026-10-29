package com.adrianausin.marketdata.candles;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import com.adrianausin.marketdata.feed.Tick;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

class CandleAggregatorTest {

    private static final Duration ONE_MINUTE = Duration.ofMinutes(1);

    @Test
    void buildsOpenHighLowCloseForEachMinute() {
        Flux<Tick> ticks = Flux.just(
                tick("AAPL", "100.00", 10, "10:30:05"),
                tick("AAPL", "101.50", 20, "10:30:20"),
                tick("AAPL", "99.80", 5, "10:30:45"),
                tick("AAPL", "100.40", 15, "10:30:59"),
                tick("AAPL", "100.90", 1, "10:31:02"));

        StepVerifier.create(CandleAggregator.aggregate(ticks, ONE_MINUTE))
                .assertNext(candle -> {
                    assertThat(candle.openTime()).isEqualTo(Instant.parse("2026-10-29T10:30:00Z"));
                    assertThat(candle.open()).isEqualByComparingTo("100.00");
                    assertThat(candle.high()).isEqualByComparingTo("101.50");
                    assertThat(candle.low()).isEqualByComparingTo("99.80");
                    assertThat(candle.close()).isEqualByComparingTo("100.40");
                    assertThat(candle.volume()).isEqualTo(50);
                    assertThat(candle.trades()).isEqualTo(4);
                    assertThat(candle.intervalSeconds()).isEqualTo(60);
                })
                .assertNext(candle -> {
                    assertThat(candle.openTime()).isEqualTo(Instant.parse("2026-10-29T10:31:00Z"));
                    assertThat(candle.open()).isEqualByComparingTo("100.90");
                    assertThat(candle.trades()).isEqualTo(1);
                })
                .verifyComplete();
    }

    @Test
    void keepsSymbolsApart() {
        Flux<Tick> ticks = Flux.just(
                tick("AAPL", "100.00", 1, "10:30:01"),
                tick("MSFT", "400.00", 1, "10:30:02"),
                tick("AAPL", "102.00", 1, "10:30:03"),
                tick("MSFT", "399.00", 1, "10:30:04"));

        List<Candle> candles = CandleAggregator.aggregate(ticks, ONE_MINUTE).collectList().block();

        assertThat(candles).extracting(Candle::symbol, Candle::high, Candle::low)
                .containsExactlyInAnyOrder(
                        tuple("AAPL", new BigDecimal("102.00"), new BigDecimal("100.00")),
                        tuple("MSFT", new BigDecimal("400.00"), new BigDecimal("399.00")));
    }

    @Test
    void emitsACandleAsSoonAsTheNextIntervalStarts() {
        Sinks.Many<Tick> live = Sinks.many().unicast().onBackpressureBuffer();

        StepVerifier.create(CandleAggregator.aggregate(live.asFlux(), ONE_MINUTE))
                .then(() -> live.tryEmitNext(tick("AAPL", "100.00", 1, "10:30:10")))
                .then(() -> live.tryEmitNext(tick("AAPL", "100.50", 1, "10:30:50")))
                .expectNoEvent(Duration.ofMillis(100))
                .then(() -> live.tryEmitNext(tick("AAPL", "101.00", 1, "10:31:00")))
                .assertNext(candle -> assertThat(candle.close()).isEqualByComparingTo("100.50"))
                .thenCancel()
                .verify();
    }

    @Test
    void usesEventTimeNotArrivalTime() {
        Sinks.Many<Tick> live = Sinks.many().unicast().onBackpressureBuffer();

        // Ticks arriving late still go to the candle of their own timestamp
        StepVerifier.create(CandleAggregator.aggregate(live.asFlux(), Duration.ofSeconds(5)))
                .then(() -> live.tryEmitNext(tick("AAPL", "100.00", 1, "10:30:01")))
                .thenAwait(Duration.ofMillis(200))
                .then(() -> live.tryEmitNext(tick("AAPL", "100.20", 1, "10:30:04")))
                .then(() -> live.tryEmitNext(tick("AAPL", "100.40", 1, "10:30:06")))
                .assertNext(candle -> {
                    assertThat(candle.openTime()).isEqualTo(Instant.parse("2026-10-29T10:30:00Z"));
                    assertThat(candle.trades()).isEqualTo(2);
                })
                .thenCancel()
                .verify();
    }

    @Test
    void alignsBucketsToTheInterval() {
        assertThat(CandleAggregator.bucketStart(Instant.parse("2026-10-29T10:34:59.999Z"), 300_000))
                .isEqualTo(Instant.parse("2026-10-29T10:30:00Z").toEpochMilli());
    }

    private static Tick tick(String symbol, String price, long volume, String time) {
        return new Tick(symbol, new BigDecimal(price), volume, Instant.parse("2026-10-29T" + time + "Z"));
    }
}
