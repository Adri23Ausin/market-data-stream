package com.adrianausin.marketdata.alerts;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Where triggered alerts are announced. In-process for a single instance, Redis pub/sub when
 * distributed, so clients connected to any instance hear about every alert.
 */
public interface AlertEvents {

    Mono<Void> publish(PriceAlert triggered);

    Flux<PriceAlert> triggered();
}
