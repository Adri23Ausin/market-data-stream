package com.adrianausin.marketdata.candles;

import java.time.Duration;
import java.time.Instant;

import com.adrianausin.marketdata.feed.Tick;
import reactor.core.publisher.Flux;

/**
 * Turns a tick stream into OHLC candles.
 * <p>
 * Candles are built on <em>event time</em>: a tick belongs to the bucket of its own timestamp
 * (e.g. 10:31:00-10:31:59 for one-minute candles), not to the moment it was processed. A candle
 * is emitted as soon as the first tick of the next bucket for the same symbol arrives, so a
 * delay in processing never moves a trade into the wrong candle.
 */
public final class CandleAggregator {

    private CandleAggregator() {
    }

    public static Flux<Candle> aggregate(Flux<Tick> ticks, Duration interval) {
        long intervalMillis = interval.toMillis();
        long intervalSeconds = interval.toSeconds();
        return ticks
                .groupBy(Tick::symbol)
                .flatMap(bySymbol -> bySymbol
                        .windowUntilChanged(tick -> bucketStart(tick.timestamp(), intervalMillis))
                        .concatMap(window -> window
                                .map(tick -> Candle.of(tick,
                                        Instant.ofEpochMilli(bucketStart(tick.timestamp(), intervalMillis)),
                                        intervalSeconds))
                                // An empty window (possible before the first tick) reduces to nothing
                                .reduce(Candle::merge)), Integer.MAX_VALUE);
    }

    static long bucketStart(Instant timestamp, long intervalMillis) {
        long millis = timestamp.toEpochMilli();
        return millis - Math.floorMod(millis, intervalMillis);
    }
}
