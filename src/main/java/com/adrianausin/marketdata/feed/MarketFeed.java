package com.adrianausin.marketdata.feed;

import reactor.core.publisher.Flux;

/**
 * The live stream of ticks every consumer (clients, candles, alerts) subscribes to.
 */
public interface MarketFeed {

    /**
     * Hot stream: subscribers only see ticks emitted after they subscribe, and a subscriber
     * that can't keep up misses ticks instead of slowing everybody down. For market data the
     * latest price matters more than every intermediate one.
     */
    Flux<Tick> ticks();
}
