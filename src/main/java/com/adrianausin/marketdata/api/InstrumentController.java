package com.adrianausin.marketdata.api;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Locale;

import com.adrianausin.marketdata.feed.LatestPrices;
import com.adrianausin.marketdata.feed.MarketProperties;
import com.adrianausin.marketdata.feed.Tick;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import org.jspecify.annotations.Nullable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/instruments")
@Tag(name = "Instruments", description = "Tradable instruments and their latest prices")
class InstrumentController {

    private final MarketProperties properties;
    private final LatestPrices latestPrices;

    InstrumentController(MarketProperties properties, LatestPrices latestPrices) {
        this.properties = properties;
        this.latestPrices = latestPrices;
    }

    @GetMapping
    @Operation(summary = "All instruments with their last traded price")
    Flux<InstrumentView> list() {
        return Flux.fromIterable(properties.instruments()).map(instrument -> {
            Tick last = latestPrices.get(instrument.symbol()).orElse(null);
            return new InstrumentView(instrument.symbol(), instrument.name(), instrument.initialPrice(),
                    last == null ? null : last.price(), last == null ? null : last.timestamp());
        });
    }

    @GetMapping("/{symbol}/price")
    @Operation(summary = "Last traded price of an instrument")
    Mono<Tick> price(@PathVariable String symbol) {
        String normalized = symbol.toUpperCase(Locale.ROOT);
        return Mono.justOrEmpty(latestPrices.get(normalized))
                .switchIfEmpty(Mono.error(() -> new ResponseStatusException(HttpStatus.NOT_FOUND,
                        "No price for " + normalized)));
    }

    record InstrumentView(String symbol, String name, BigDecimal initialPrice, @Nullable BigDecimal lastPrice,
                          @Nullable Instant lastTradeAt) {
    }
}
