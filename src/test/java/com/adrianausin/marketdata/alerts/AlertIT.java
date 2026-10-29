package com.adrianausin.marketdata.alerts;

import java.time.Duration;

import com.adrianausin.marketdata.TestcontainersConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webtestclient.autoconfigure.AutoConfigureWebTestClient;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.web.reactive.server.WebTestClient;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "market.feed.tick-interval=20ms"
})
@AutoConfigureWebTestClient
@Import(TestcontainersConfiguration.class)
class AlertIT {

    @Autowired
    WebTestClient client;

    @Autowired
    DatabaseClient db;

    @BeforeEach
    void clean() {
        db.sql("DELETE FROM price_alerts").then().block();
    }

    @Test
    void anAlertFiresOnceAndIsStreamedToClients() {
        Flux<PriceAlert> stream = client.get().uri("/api/v1/alerts/stream")
                .accept(MediaType.TEXT_EVENT_STREAM)
                .exchange()
                .returnResult(PriceAlert.class)
                .getResponseBody();

        StepVerifier.create(stream.take(1))
                // Any AAPL price is above 1, so the next tick fires it
                .then(() -> create("{\"symbol\": \"aapl\", \"direction\": \"ABOVE\", \"price\": 1}"))
                .assertNext(alert -> {
                    assertThat(alert.symbol()).isEqualTo("AAPL");
                    assertThat(alert.status()).isEqualTo(PriceAlert.Status.TRIGGERED);
                    assertThat(alert.triggeredPrice()).isGreaterThan(alert.targetPrice());
                    assertThat(alert.triggeredAt()).isNotNull();
                })
                .expectComplete()
                .verify(Duration.ofSeconds(10));

        client.get().uri("/api/v1/alerts?status=TRIGGERED").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$.length()").isEqualTo(1);
    }

    @Test
    void anAlertThatIsNotReachedStaysActive() throws InterruptedException {
        create("{\"symbol\": \"MSFT\", \"direction\": \"BELOW\", \"price\": 0.01}");

        Thread.sleep(500);

        client.get().uri("/api/v1/alerts?status=ACTIVE").exchange()
                .expectStatus().isOk()
                .expectBody().jsonPath("$[0].symbol").isEqualTo("MSFT");
    }

    @Test
    void alertsCanBeDeleted() {
        long id = client.post().uri("/api/v1/alerts").contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"symbol\": \"NVDA\", \"direction\": \"BELOW\", \"price\": 0.01}")
                .exchange()
                .expectStatus().isCreated()
                .returnResult(PriceAlert.class).getResponseBody().blockFirst().id();

        client.delete().uri("/api/v1/alerts/{id}", id).exchange().expectStatus().isNoContent();
        client.delete().uri("/api/v1/alerts/{id}", id).exchange().expectStatus().isNotFound();
    }

    @Test
    void validatesNewAlerts() {
        client.post().uri("/api/v1/alerts").contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"symbol\": \"TSLA\", \"direction\": \"ABOVE\", \"price\": 100}")
                .exchange()
                .expectStatus().isBadRequest();
        client.post().uri("/api/v1/alerts").contentType(MediaType.APPLICATION_JSON)
                .bodyValue("{\"symbol\": \"AAPL\", \"direction\": \"ABOVE\", \"price\": -5}")
                .exchange()
                .expectStatus().isBadRequest();
    }

    private void create(String json) {
        client.post().uri("/api/v1/alerts").contentType(MediaType.APPLICATION_JSON).bodyValue(json)
                .exchange()
                .expectStatus().isCreated();
    }
}
