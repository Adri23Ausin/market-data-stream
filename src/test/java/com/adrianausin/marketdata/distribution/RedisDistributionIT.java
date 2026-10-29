package com.adrianausin.marketdata.distribution;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;

import com.adrianausin.marketdata.TestcontainersConfiguration;
import com.adrianausin.marketdata.feed.LatestPrices;
import com.adrianausin.marketdata.feed.MarketFeed;
import com.adrianausin.marketdata.feed.Tick;
import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Runs an instance that does not generate prices itself ({@code market.feed.enabled=false}),
 * like a scaled-out replica, and plays the generator by publishing to Redis directly.
 */
@SpringBootTest(properties = {
        "market.distribution=redis",
        "market.feed.enabled=false"
})
@Testcontainers
@Import(TestcontainersConfiguration.class)
class RedisDistributionIT {

    @Container
    @ServiceConnection
    static RedisContainer redis = new RedisContainer("redis:7.4-alpine");

    @Autowired
    MarketFeed feed;

    @Autowired
    LatestPrices latestPrices;

    @Autowired
    ReactiveStringRedisTemplate redisTemplate;

    @Autowired
    JsonMapper jsonMapper;

    @Test
    void streamsTicksPublishedOnRedisByAnotherInstance() {
        Tick tick = new Tick("NVDA", new BigDecimal("140.25"), 30, Instant.parse("2026-10-29T10:00:00Z"));

        StepVerifier.create(feed.ticks().take(1))
                .then(() -> redisTemplate.convertAndSend(RedisChannels.TICKS, jsonMapper.writeValueAsString(tick))
                        .delaySubscription(Duration.ofMillis(300)).subscribe())
                .expectNext(tick)
                .expectComplete()
                .verify(Duration.ofSeconds(5));

        assertThat(feed).isInstanceOf(RedisMarketFeed.class);
        assertThat(latestPrices.get("NVDA")).contains(tick);
    }
}
