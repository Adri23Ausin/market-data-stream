package com.adrianausin.marketdata.feed;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.adrianausin.marketdata.feed.MarketProperties.Instrument;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;

import static org.assertj.core.api.Assertions.assertThat;

class PriceSimulatorTest {

    private static final List<Instrument> INSTRUMENTS = List.of(
            new Instrument("AAPL", "Apple", new BigDecimal("228.50"), 0.10, 0.25, 2),
            new Instrument("SAN.MC", "Santander", new BigDecimal("4.620"), 0.05, 0.30, 3));

    @Test
    void emitsOneTickPerInstrumentAtEveryInterval() {
        StepVerifier.withVirtualTime(() -> simulator(42).ticks().take(4))
                .expectSubscription()
                .expectNoEvent(Duration.ofMillis(250))
                .assertNext(tick -> assertThat(tick.symbol()).isEqualTo("AAPL"))
                .assertNext(tick -> assertThat(tick.symbol()).isEqualTo("SAN.MC"))
                .expectNoEvent(Duration.ofMillis(250))
                .expectNextCount(2)
                .verifyComplete();
    }

    @Test
    void timestampsFollowTheSchedulerClock() {
        StepVerifier.withVirtualTime(() -> simulator(42).ticks().skip(2).take(1))
                .thenAwait(Duration.ofMillis(500))
                .assertNext(tick -> assertThat(tick.timestamp().toEpochMilli())
                        .isEqualTo(VirtualTimeScheduler.get().now(TimeUnit.MILLISECONDS)))
                .verifyComplete();
    }

    @Test
    void pricesUseTheInstrumentPrecisionAndStayPositive() {
        StepVerifier.withVirtualTime(() -> simulator(7).ticks().take(400))
                .thenAwait(Duration.ofSeconds(60))
                .recordWith(ArrayList::new)
                .expectNextCount(400)
                .consumeRecordedWith(ticks -> assertThat(ticks).allSatisfy(tick -> {
                    assertThat(tick.price()).isPositive();
                    assertThat(tick.price().scale()).isEqualTo(tick.symbol().equals("SAN.MC") ? 3 : 2);
                    assertThat(tick.volume()).isBetween(1L, 500L);
                }))
                .verifyComplete();
    }

    @Test
    void theSameSeedProducesTheSamePrices() {
        List<BigDecimal> first = prices(1234);
        List<BigDecimal> second = prices(1234);

        assertThat(first).hasSize(20).isEqualTo(second);
        assertThat(first.stream().distinct().count()).as("prices actually move").isGreaterThan(1);
    }

    private static List<BigDecimal> prices(long seed) {
        VirtualTimeScheduler scheduler = VirtualTimeScheduler.create();
        PriceSimulator simulator = new PriceSimulator(INSTRUMENTS.subList(0, 1), Duration.ofMillis(250), scheduler,
                new Random(seed));
        List<BigDecimal> prices = new ArrayList<>();
        simulator.ticks().take(20).map(Tick::price).subscribe(prices::add);
        scheduler.advanceTimeBy(Duration.ofSeconds(5));
        return prices;
    }

    private static PriceSimulator simulator(long seed) {
        return new PriceSimulator(INSTRUMENTS, Duration.ofMillis(250), VirtualTimeScheduler.get(), new Random(seed));
    }
}
