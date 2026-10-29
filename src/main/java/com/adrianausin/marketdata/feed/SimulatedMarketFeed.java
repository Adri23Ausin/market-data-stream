package com.adrianausin.marketdata.feed;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

/**
 * Runs the {@link PriceSimulator} for the whole life of the application and multicasts its
 * ticks through a best-effort sink: no subscriber can apply backpressure on the generator.
 */
public class SimulatedMarketFeed implements MarketFeed, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(SimulatedMarketFeed.class);

    private final PriceSimulator simulator;
    private final Sinks.Many<Tick> sink = Sinks.many().multicast().directBestEffort();
    private volatile Disposable subscription;

    public SimulatedMarketFeed(MarketProperties properties) {
        Long seed = properties.feed().seed();
        this.simulator = new PriceSimulator(properties.instruments(), properties.feed().tickInterval(),
                Schedulers.parallel(), seed == null ? new Random() : new Random(seed));
    }

    @Override
    public Flux<Tick> ticks() {
        return MarketFeed.buffered(sink.asFlux());
    }

    @Override
    public void start() {
        // tryEmitNext: having no subscriber at a given moment is normal, the tick is just dropped
        subscription = simulator.ticks().subscribe(
                sink::tryEmitNext,
                error -> log.error("Price simulator stopped", error));
        log.info("Simulated market feed started");
    }

    @Override
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
        sink.tryEmitComplete();
    }

    @Override
    public boolean isRunning() {
        return subscription != null && !subscription.isDisposed();
    }
}
