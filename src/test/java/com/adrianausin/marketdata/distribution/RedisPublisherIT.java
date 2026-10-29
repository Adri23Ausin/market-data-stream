package com.adrianausin.marketdata.distribution;

import java.time.Duration;
import java.util.stream.Collectors;

import com.adrianausin.marketdata.feed.MarketFeed;
import com.adrianausin.marketdata.feed.Tick;
import com.redis.testcontainers.RedisContainer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The generating instance: its own simulated ticks make the round trip through Redis.
 */
@SpringBootTest(properties = {
        "market.distribution=redis",
        "market.feed.enabled=true",
        "market.feed.tick-interval=50ms"
})
@Testcontainers
class RedisPublisherIT {

    @Container
    @ServiceConnection
    static RedisContainer redis = new RedisContainer("redis:7.4-alpine");

    @Autowired
    MarketFeed feed;

    @Autowired
    ReactiveStringRedisTemplate redisTemplate;

    @Test
    void publishesSimulatedTicksAndKeepsTheLatestPriceOfEachSymbol() {
        StepVerifier.create(feed.ticks().map(Tick::symbol).take(12).collect(Collectors.toSet()))
                .assertNext(symbols -> assertThat(symbols).hasSize(6))
                .expectComplete()
                .verify(Duration.ofSeconds(5));

        Long symbols = redisTemplate.opsForHash().size(RedisChannels.LATEST).block();
        assertThat(symbols).isEqualTo(6);
    }
}
