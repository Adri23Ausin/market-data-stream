package com.adrianausin.marketdata.candles;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.adrianausin.marketdata.feed.MarketFeed;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

/**
 * Live candle streams for the supported intervals. Each interval is aggregated once and shared
 * by every subscriber, instead of once per connected client.
 */
@Component
public class CandleStreams {

    public static final List<Duration> INTERVALS = List.of(Duration.ofSeconds(5), Duration.ofMinutes(1));

    private final Map<Duration, Flux<Candle>> streams;

    public CandleStreams(MarketFeed feed) {
        this.streams = INTERVALS.stream().collect(Collectors.toUnmodifiableMap(Function.identity(),
                interval -> CandleAggregator.aggregate(feed.ticks(), interval).share()));
    }

    public Flux<Candle> candles(Duration interval) {
        Flux<Candle> stream = streams.get(interval);
        if (stream == null) {
            throw new IllegalArgumentException("Unsupported interval %s, use one of %s".formatted(interval, INTERVALS));
        }
        return stream;
    }
}
