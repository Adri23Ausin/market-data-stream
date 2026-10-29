package com.adrianausin.marketdata.api;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.adrianausin.marketdata.TestcontainersConfiguration;
import com.adrianausin.marketdata.candles.Candle;
import com.adrianausin.marketdata.feed.Tick;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.client.ReactorNettyWebSocketClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "market.feed.tick-interval=20ms",
        "market.feed.seed=42"
})
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class StreamApiIT {

    @Autowired
    WebTestClient client;

    @Autowired
    JsonMapper jsonMapper;

    @LocalServerPort
    int port;

    @Test
    void streamsTicksOfTheRequestedSymbols() {
        Flux<Tick> ticks = client.get().uri("/api/v1/stream/ticks?symbols=aapl,NVDA")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isOk()
                .returnResult(Tick.class)
                .getResponseBody();

        StepVerifier.create(ticks.take(20))
                .thenConsumeWhile(tick -> List.of("AAPL", "NVDA").contains(tick.symbol()))
                .verifyComplete();
    }

    @Test
    void throttlesEachSymbolSeparately() {
        Flux<Tick> ticks = client.get().uri("/api/v1/stream/ticks?symbols=AAPL,MSFT&throttleMs=200")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .returnResult(Tick.class)
                .getResponseBody();

        List<Tick> received = ticks.take(Duration.ofMillis(1100)).collectList().block(Duration.ofSeconds(5));

        // Snapshot (one per symbol) + about 5 samples per symbol, instead of ~50 raw ticks each
        assertThat(received).extracting(Tick::symbol).contains("AAPL", "MSFT");
        assertThat(received.stream().filter(tick -> tick.symbol().equals("AAPL")).count()).isBetween(3L, 8L);
    }

    @Test
    void streamsCandlesForTheSupportedIntervals() {
        Flux<Candle> candles = client.get().uri("/api/v1/stream/candles?interval=5s&symbols=AAPL")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .returnResult(Candle.class)
                .getResponseBody();

        StepVerifier.create(candles.take(1))
                .assertNext(candle -> {
                    assertThat(candle.symbol()).isEqualTo("AAPL");
                    assertThat(candle.intervalSeconds()).isEqualTo(5);
                    assertThat(candle.high()).isGreaterThanOrEqualTo(candle.low());
                    assertThat(candle.trades()).isPositive();
                })
                .expectComplete()
                .verify(Duration.ofSeconds(12));
    }

    @Test
    void rejectsUnsupportedIntervals() {
        client.get().uri("/api/v1/stream/candles?interval=7m")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .expectStatus().isBadRequest();
    }

    @Test
    void returnsTheLatestPriceOfAnInstrument() throws InterruptedException {
        Thread.sleep(100);

        client.get().uri("/api/v1/instruments/aapl/price").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.symbol").isEqualTo("AAPL");
        client.get().uri("/api/v1/instruments/UNKNOWN/price").exchange()
                .expectStatus().isNotFound();
        client.get().uri("/api/v1/instruments").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(6);
    }

    @Test
    void webSocketClientsCanChangeTheirSubscriptions() {
        List<String> received = new CopyOnWriteArrayList<>();

        new ReactorNettyWebSocketClient().execute(URI.create("ws://localhost:" + port + "/ws/ticks"), session -> {
            Mono<Void> send = session.send(Flux.just(
                            "not json",
                            "{\"action\": \"subscribe\", \"symbols\": [\"msft\"]}")
                    .map(session::textMessage));
            Mono<Void> receive = session.receive()
                    .map(WebSocketMessage::getPayloadAsText)
                    .map(json -> jsonMapper.readValue(json, Tick.class).symbol())
                    .doOnNext(received::add)
                    .take(5)
                    .then();
            return send.then(receive);
        }).block(Duration.ofSeconds(5));

        assertThat(received).hasSize(5).containsOnly("MSFT");
    }
}
