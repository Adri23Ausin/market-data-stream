package com.adrianausin.marketdata.feed;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import com.adrianausin.marketdata.feed.MarketProperties.Feed;
import com.adrianausin.marketdata.feed.MarketProperties.Instrument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class SimulatedMarketFeedTest {

    private final SimulatedMarketFeed feed = new SimulatedMarketFeed(new MarketProperties(
            new Feed(true, Duration.ofMillis(20), 1L),
            List.of(new Instrument("AAPL", "Apple", new BigDecimal("228.50"), 0.1, 0.25, 2))));

    @AfterEach
    void stop() {
        feed.stop();
    }

    @Test
    void isHotAndSharedBetweenSubscribers() {
        feed.start();

        StepVerifier.create(feed.ticks().take(3)).expectNextCount(3).verifyComplete();
        StepVerifier.create(feed.ticks().take(3)).expectNextCount(3).verifyComplete();
        assertThat(feed.isRunning()).isTrue();
    }

    @Test
    void aSlowSubscriberDoesNotBlockTheOthers() {
        feed.start();
        // Requests a single tick and never asks for more
        feed.ticks().subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                subscription.request(1);
            }
        });

        StepVerifier.create(feed.ticks().take(10))
                .expectNextCount(10)
                .expectComplete()
                .verify(Duration.ofSeconds(2));
    }

    @Test
    void completesSubscribersWhenStopped() {
        feed.start();

        StepVerifier.create(feed.ticks())
                .then(feed::stop)
                .expectComplete()
                .verify(Duration.ofSeconds(1));
    }
}
