package com.adrianausin.marketdata.feed;

import reactor.core.publisher.BufferOverflowStrategy;
import reactor.core.publisher.Flux;

/**
 * The live stream of ticks every consumer (clients, candles, alerts) subscribes to.
 */
public interface MarketFeed {

    /** Ticks a subscriber can fall behind before the oldest ones are dropped. */
    int SUBSCRIBER_BUFFER = 256;

    /**
     * Hot stream: subscribers only see ticks emitted after they subscribe, and a subscriber
     * that can't keep up misses the oldest ticks instead of slowing everybody down. For market
     * data the latest price matters more than every intermediate one.
     */
    Flux<Tick> ticks();

    /**
     * Gives each subscriber a small buffer. Without it, a subscriber that requests one tick at a
     * time (like {@code concatMap} or a network writer) would lose the rest of every burst, since
     * all instruments tick at the same moment.
     */
    static Flux<Tick> buffered(Flux<Tick> hotTicks) {
        return hotTicks.onBackpressureBuffer(SUBSCRIBER_BUFFER, dropped -> { }, BufferOverflowStrategy.DROP_OLDEST);
    }
}
