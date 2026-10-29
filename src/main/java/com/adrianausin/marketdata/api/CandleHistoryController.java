package com.adrianausin.marketdata.api;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

import com.adrianausin.marketdata.candles.Candle;
import com.adrianausin.marketdata.history.CandleRepository;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.jspecify.annotations.Nullable;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/v1/candles")
@Tag(name = "History", description = "Stored candles")
class CandleHistoryController {

    private final CandleRepository repository;

    CandleHistoryController(CandleRepository repository) {
        this.repository = repository;
    }

    @GetMapping("/{symbol}")
    @Operation(summary = "Stored candles of a symbol, oldest first (the latest `limit` ones in the range)")
    Flux<Candle> history(
            @PathVariable String symbol,
            @RequestParam(defaultValue = "1m") String interval,
            @RequestParam(required = false) @Nullable Instant from,
            @RequestParam(required = false) @Nullable Instant to,
            @RequestParam(defaultValue = "120") @Min(1) @Max(1000) int limit) {
        Duration duration = Intervals.parse(interval);
        Instant end = to == null ? Instant.now() : to;
        Instant start = from == null ? end.minus(duration.multipliedBy(limit)) : from;
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("'from' must be before 'to'");
        }
        return repository.find(symbol.toUpperCase(Locale.ROOT), duration.toSeconds(), start, end, limit);
    }
}
