package com.adrianausin.marketdata.history;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.stream.IntStream;

import com.adrianausin.marketdata.TestcontainersConfiguration;
import com.adrianausin.marketdata.candles.Candle;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "market.feed.tick-interval=20ms",
        "market.history.intervals=5s"
})
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class CandleHistoryIT {

    private static final Instant T0 = Instant.parse("2026-10-28T09:00:00Z");

    @Autowired
    CandleRepository repository;

    @Autowired
    DatabaseClient db;

    @Autowired
    WebTestClient client;

    @BeforeEach
    void clean() {
        db.sql("DELETE FROM candles WHERE open_time < '2026-10-29'").then().block();
    }

    @Test
    void storesCandlesAndIgnoresReplays() {
        Candle candle = candle("AAPL", T0, "228.10", "229.00", "227.90", "228.75");

        StepVerifier.create(repository.saveAll(List.of(candle, candle("MSFT", T0, "430", "431", "429", "430.5"))))
                .expectNext(2L)
                .verifyComplete();
        StepVerifier.create(repository.saveAll(List.of(candle)))
                .expectNext(0L)
                .verifyComplete();

        StepVerifier.create(repository.find("AAPL", 60, T0, T0.plusSeconds(60), 10))
                .assertNext(stored -> {
                    assertThat(stored.symbol()).isEqualTo("AAPL");
                    assertThat(stored.openTime()).isEqualTo(T0);
                    assertThat(stored.high()).isEqualByComparingTo("229.00");
                    assertThat(stored.close()).isEqualByComparingTo("228.75");
                    assertThat(stored.volume()).isEqualTo(1000);
                })
                .verifyComplete();
    }

    @Test
    void returnsTheLatestCandlesOldestFirst() {
        List<Candle> hour = IntStream.range(0, 60)
                .mapToObj(minute -> candle("NVDA", T0.plusSeconds(60L * minute), "100", "101", "99", "100.5"))
                .toList();
        repository.saveAll(hour).block();

        client.get().uri("/api/v1/candles/nvda?interval=1m&from={from}&to={to}&limit=5", T0, T0.plusSeconds(3600))
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(Candle.class)
                .value(candles -> assertThat(candles).extracting(Candle::openTime)
                        .containsExactly(T0.plusSeconds(55 * 60), T0.plusSeconds(56 * 60), T0.plusSeconds(57 * 60),
                                T0.plusSeconds(58 * 60), T0.plusSeconds(59 * 60)));
    }

    @Test
    void rejectsAnEmptyRange() {
        client.get().uri("/api/v1/candles/AAPL?from={from}&to={to}", T0, T0)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void recordsLiveCandlesAsTheyClose() {
        Mono<Long> stored = db.sql("SELECT count(*) AS n FROM candles WHERE interval_seconds = 5")
                .map(row -> row.get("n", Long.class))
                .one();

        // The first 5-second candle closes at most ~5s after startup, then it is flushed in a batch
        StepVerifier.create(stored.repeatWhenEmpty(r -> r.delayElements(Duration.ofMillis(500)))
                        .filter(count -> count > 0)
                        .repeatWhenEmpty(20, r -> r.delayElements(Duration.ofMillis(500))))
                .assertNext(count -> assertThat(count).isGreaterThanOrEqualTo(6))
                .verifyComplete();
    }

    private static Candle candle(String symbol, Instant openTime, String open, String high, String low, String close) {
        return new Candle(symbol, openTime, 60, new BigDecimal(open), new BigDecimal(high), new BigDecimal(low),
                new BigDecimal(close), 1000, 42);
    }
}
