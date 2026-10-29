package com.adrianausin.marketdata.alerts;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

class LocalAlertEvents implements AlertEvents {

    private final Sinks.Many<PriceAlert> sink = Sinks.many().multicast().directBestEffort();

    @Override
    public Mono<Void> publish(PriceAlert triggered) {
        return Mono.fromRunnable(() -> sink.tryEmitNext(triggered));
    }

    @Override
    public Flux<PriceAlert> triggered() {
        return sink.asFlux().onBackpressureBuffer(64);
    }
}
