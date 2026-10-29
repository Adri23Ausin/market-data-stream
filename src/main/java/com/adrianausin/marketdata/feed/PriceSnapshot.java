package com.adrianausin.marketdata.feed;

import reactor.core.publisher.Flux;

/**
 * Shared store of the latest tick per symbol, read when an instance starts.
 */
public interface PriceSnapshot {

    Flux<Tick> latest();
}
