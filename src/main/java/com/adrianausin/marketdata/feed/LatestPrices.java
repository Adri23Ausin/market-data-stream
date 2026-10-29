package com.adrianausin.marketdata.feed;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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
    private final Map<String, Tick> latest = new ConcurrentHashMap<>();
    private volatile Disposable subscription;

    public LatestPrices(MarketFeed feed) {
        this.feed = feed;
    }

    public Optional<Tick> get(String symbol) {
        return Optional.ofNullable(latest.get(symbol));
    }

    public Map<String, Tick> all() {
        return Map.copyOf(latest);
    }

    @Override
    public void start() {
        subscription = feed.ticks().subscribe(tick -> latest.put(tick.symbol(), tick));
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
     * Start after the feed, so the first ticks are not missed.
     */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 1;
    }
}
