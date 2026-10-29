package com.adrianausin.marketdata.feed;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;

/**
 * Last traded price of every symbol, so a client gets a price immediately instead of waiting
 * for the next tick.
 */
@Component
public class LatestPrices implements SmartLifecycle {

    private final MarketFeed feed;
    private final ObjectProvider<PriceSnapshot> snapshot;
    private final Map<String, Tick> latest = new ConcurrentHashMap<>();
    private volatile Disposable subscription;

    public LatestPrices(MarketFeed feed, ObjectProvider<PriceSnapshot> snapshot) {
        this.feed = feed;
        this.snapshot = snapshot;
    }

    public Optional<Tick> get(String symbol) {
        return Optional.ofNullable(latest.get(symbol));
    }

    public Map<String, Tick> all() {
        return Map.copyOf(latest);
    }

    @Override
    public void start() {
        subscription = feed.ticks().subscribe(this::update);
        // Distributed mode: prices published before this instance started
        snapshot.ifAvailable(store -> store.latest().subscribe(this::update));
    }

    private void update(Tick tick) {
        // A snapshot read late must never overwrite a newer live tick
        latest.merge(tick.symbol(), tick,
                (current, candidate) -> candidate.timestamp().isBefore(current.timestamp()) ? current : candidate);
    }

    @Override
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return subscription != null && !subscription.isDisposed();
    }

    /**
     * A lower phase starts earlier: subscribe before the feed starts, so no tick is missed.
     */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 1;
    }
}
