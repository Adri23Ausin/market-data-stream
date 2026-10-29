package com.adrianausin.marketdata.history;

import java.time.Duration;

import com.adrianausin.marketdata.candles.CandleStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

/**
 * Stores closed candles. Runs only on the instance that generates prices, so a scaled-out
 * deployment doesn't write every candle once per instance.
 * <p>
 * Candles are written in batches ({@code bufferTimeout}: up to 100 candles or every 2 seconds,
 * whichever comes first), and a database hiccup drops a batch with a warning instead of
 * stopping the recording for good.
 */
@Component
@ConditionalOnBooleanProperty(name = "market.feed.enabled", matchIfMissing = true)
class CandleRecorder implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(CandleRecorder.class);

    private final CandleStreams candleStreams;
    private final CandleRepository repository;
    private final HistoryProperties properties;
    private volatile Disposable subscription;

    CandleRecorder(CandleStreams candleStreams, CandleRepository repository, HistoryProperties properties) {
        this.candleStreams = candleStreams;
        this.repository = repository;
        this.properties = properties;
    }

    @Override
    public void start() {
        subscription = Flux.fromIterable(properties.intervals())
                .flatMap(candleStreams::candles)
                .bufferTimeout(100, Duration.ofSeconds(2))
                .concatMap(batch -> repository.saveAll(batch)
                        .doOnError(error -> log.warn("Could not store {} candles: {}", batch.size(), error.toString()))
                        .onErrorReturn(0L))
                .subscribe();
        log.info("Recording {} candles", properties.intervals());
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
     * A lower phase starts earlier: subscribe before the feed produces its first ticks.
     */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 1;
    }
}
