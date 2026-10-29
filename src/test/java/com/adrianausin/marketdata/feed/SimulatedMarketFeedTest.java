package com.adrianausin.marketdata.feed;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import com.adrianausin.marketdata.feed.MarketProperties.Feed;
import com.adrianausin.marketdata.feed.MarketProperties.Instrument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class SimulatedMarketFeedTest {

    private final SimulatedMarketFeed feed = new SimulatedMarketFeed(new MarketProperties(
            new Feed(true, Duration.ofMillis(20), 1L),
            List.of(new Instrument("AAPL", "Apple", new BigDecimal("228.50"), 0.1, 0.25, 2),
                    new Instrument("MSFT", "Microsoft", new BigDecimal("431.20"), 0.09, 0.22, 2),
                    new Instrument("NVDA", "NVIDIA", new BigDecimal("138.70"), 0.2, 0.55, 2))));

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
    void subscribersRequestingOneTickAtATimeStillGetEveryInstrument() {
        feed.start();

        // concatMap only requests the next tick once the previous one has been handled
        StepVerifier.create(feed.ticks()
                        .concatMap(tick -> Mono.just(tick).delayElement(Duration.ofMillis(1)))
                        .map(Tick::symbol)
                        .take(9)
                        .collect(Collectors.toSet()))
                .assertNext(symbols -> assertThat(symbols).containsExactlyInAnyOrder("AAPL", "MSFT", "NVDA"))
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
