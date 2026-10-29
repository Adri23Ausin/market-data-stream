package com.adrianausin.marketdata.api;

import java.time.Duration;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import com.adrianausin.marketdata.candles.Candle;
import com.adrianausin.marketdata.candles.CandleStreams;
import com.adrianausin.marketdata.feed.LatestPrices;
import com.adrianausin.marketdata.feed.MarketFeed;
import com.adrianausin.marketdata.feed.Tick;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.jspecify.annotations.Nullable;
import org.springframework.http.MediaType;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * Server-Sent Events endpoints. SSE fits one-way market data well: plain HTTP, automatic
 * reconnection in browsers ({@code EventSource}) and no extra protocol.
 */
@RestController
@RequestMapping(path = "/api/v1/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
@Tag(name = "Streams", description = "Live market data over Server-Sent Events")
class StreamController {

    /** Comment sent when nothing else was, so proxies don't close idle connections. */
    static final Duration HEARTBEAT = Duration.ofSeconds(15);

    private final MarketFeed feed;
    private final LatestPrices latestPrices;
    private final CandleStreams candleStreams;

    StreamController(MarketFeed feed, LatestPrices latestPrices, CandleStreams candleStreams) {
        this.feed = feed;
        this.latestPrices = latestPrices;
        this.candleStreams = candleStreams;
    }

    @GetMapping("/ticks")
    @Operation(summary = "Live ticks, starting with the latest price of each requested symbol")
    Flux<ServerSentEvent<Tick>> ticks(
            @Parameter(description = "Comma-separated symbols, all when omitted")
            @RequestParam(required = false) @Nullable Set<String> symbols,
            @Parameter(description = "At most one tick per symbol per this many milliseconds")
            @RequestParam(required = false) @Nullable Long throttleMs) {
        Set<String> wanted = normalize(symbols);
        Flux<Tick> snapshot = Flux.fromIterable(latestPrices.all().values()).filter(tick -> matches(wanted, tick));
        Flux<Tick> live = feed.ticks().filter(tick -> matches(wanted, tick));
        if (throttleMs != null && throttleMs > 0) {
            Duration period = Duration.ofMillis(throttleMs);
            // Throttle each symbol separately, so a busy symbol never hides a quiet one
            live = live.groupBy(Tick::symbol).flatMap(bySymbol -> bySymbol.sample(period));
        }
        return withHeartbeat(Flux.concat(snapshot, live)
                .map(tick -> ServerSentEvent.builder(tick).event("tick").build()));
    }

    @GetMapping("/candles")
    @Operation(summary = "Candles as they close, for an interval of 5s or 1m")
    Flux<ServerSentEvent<Candle>> candles(
            @RequestParam(defaultValue = "1m") String interval,
            @RequestParam(required = false) @Nullable Set<String> symbols) {
        Set<String> wanted = normalize(symbols);
        return withHeartbeat(candleStreams.candles(Intervals.parse(interval))
                .filter(candle -> wanted.isEmpty() || wanted.contains(candle.symbol()))
                .map(candle -> ServerSentEvent.builder(candle).event("candle").build()));
    }

    private static <T> Flux<ServerSentEvent<T>> withHeartbeat(Flux<ServerSentEvent<T>> events) {
        Flux<ServerSentEvent<T>> heartbeat = Flux.interval(HEARTBEAT)
                .map(i -> ServerSentEvent.<T>builder().comment("heartbeat").build());
        return Flux.merge(events, heartbeat);
    }

    private static Set<String> normalize(@Nullable Set<String> symbols) {
        if (symbols == null) {
            return Set.of();
        }
        return symbols.stream().map(s -> s.strip().toUpperCase(Locale.ROOT)).filter(s -> !s.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static boolean matches(Set<String> wanted, Tick tick) {
        return wanted.isEmpty() || wanted.contains(tick.symbol());
    }
}
