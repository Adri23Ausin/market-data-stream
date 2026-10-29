package com.adrianausin.marketdata.alerts;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import com.adrianausin.marketdata.feed.MarketFeed;
import com.adrianausin.marketdata.feed.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Checks every tick against the active alerts of its symbol.
 * <p>
 * Active alerts are kept in memory, indexed by symbol, so evaluating a tick costs a map lookup
 * instead of a database query. The index is reloaded every few seconds (alerts can be created
 * on any instance) and updated immediately for alerts created or deleted here. Firing is done
 * with a conditional UPDATE, so an alert fires exactly once even if several ticks cross the
 * target at the same time.
 */
@Component
@ConditionalOnBooleanProperty(name = "market.feed.enabled", matchIfMissing = true)
public class AlertEngine implements SmartLifecycle {

    static final Duration RELOAD_INTERVAL = Duration.ofSeconds(5);

    private static final Logger log = LoggerFactory.getLogger(AlertEngine.class);

    private final MarketFeed feed;
    private final PriceAlertRepository repository;
    private final AlertEvents events;
    private volatile Map<String, List<PriceAlert>> activeBySymbol = Map.of();
    private final Map<Long, Boolean> firing = new ConcurrentHashMap<>();
    private volatile Disposable subscriptions;

    public AlertEngine(MarketFeed feed, PriceAlertRepository repository, AlertEvents events) {
        this.feed = feed;
        this.repository = repository;
        this.events = events;
    }

    /**
     * Picks up alert changes right away instead of waiting for the next reload.
     */
    public Mono<Void> refresh() {
        return repository.findAll(PriceAlert.Status.ACTIVE)
                .collect(Collectors.groupingBy(PriceAlert::symbol))
                .doOnNext(index -> activeBySymbol = Map.copyOf(index))
                .then();
    }

    @Override
    public void start() {
        Disposable reload = Flux.interval(Duration.ZERO, RELOAD_INTERVAL)
                .concatMap(i -> refresh().onErrorResume(error -> {
                    log.warn("Could not reload active alerts: {}", error.toString());
                    return Mono.empty();
                }))
                .subscribe();
        Disposable evaluation = feed.ticks()
                .flatMap(this::fire)
                .subscribe(alert -> log.info("Alert {} fired: {} {} {} at {}", alert.id(), alert.symbol(),
                        alert.direction(), alert.targetPrice(), alert.triggeredPrice()));
        subscriptions = Disposables.composite(reload, evaluation);
    }

    private Flux<PriceAlert> fire(Tick tick) {
        List<PriceAlert> candidates = activeBySymbol.getOrDefault(tick.symbol(), List.of());
        return Flux.fromIterable(candidates)
                .filter(alert -> alert.isMetBy(tick.price()))
                // Skip alerts whose UPDATE is already in flight for an earlier tick
                .filter(alert -> firing.putIfAbsent(alert.id(), Boolean.TRUE) == null)
                .concatMap(alert -> repository.trigger(alert.id(), tick.price(), tick.timestamp())
                        .flatMap(triggered -> events.publish(triggered).thenReturn(triggered))
                        .doFinally(signal -> forget(alert)))
                .onErrorResume(error -> {
                    log.warn("Could not fire alerts for {}: {}", tick.symbol(), error.toString());
                    return Flux.empty();
                });
    }

    private void forget(PriceAlert alert) {
        activeBySymbol = activeBySymbol.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey,
                entry -> entry.getValue().stream().filter(a -> a.id() != alert.id()).toList()));
        firing.remove(alert.id());
    }

    @Override
    public void stop() {
        if (subscriptions != null) {
            subscriptions.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return subscriptions != null && !subscriptions.isDisposed();
    }
}
