package com.adrianausin.marketdata.api;

import java.math.BigDecimal;
import java.net.URI;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import com.adrianausin.marketdata.alerts.AlertEngine;
import com.adrianausin.marketdata.alerts.AlertEvents;
import com.adrianausin.marketdata.alerts.PriceAlert;
import com.adrianausin.marketdata.alerts.PriceAlertRepository;
import com.adrianausin.marketdata.feed.MarketProperties;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Digits;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.codec.ServerSentEvent;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/v1/alerts")
@Tag(name = "Alerts", description = "Price alerts that fire once when a target is reached")
class AlertController {

    private final PriceAlertRepository repository;
    private final AlertEvents events;
    private final ObjectProvider<AlertEngine> engine;
    private final Set<String> symbols;

    AlertController(PriceAlertRepository repository, AlertEvents events, ObjectProvider<AlertEngine> engine,
                    MarketProperties properties) {
        this.repository = repository;
        this.events = events;
        this.engine = engine;
        this.symbols = properties.instruments().stream().map(MarketProperties.Instrument::symbol)
                .collect(Collectors.toUnmodifiableSet());
    }

    @PostMapping
    @Operation(summary = "Create an alert, e.g. NVDA ABOVE 150")
    Mono<ResponseEntity<PriceAlert>> create(@Valid @RequestBody NewAlert request) {
        String symbol = request.symbol().toUpperCase(Locale.ROOT);
        if (!symbols.contains(symbol)) {
            return Mono.error(new IllegalArgumentException("Unknown symbol " + symbol));
        }
        return repository.create(symbol, request.direction(), request.price())
                .flatMap(alert -> refreshEngine().thenReturn(alert))
                .map(alert -> ResponseEntity.created(URI.create("/api/v1/alerts/" + alert.id())).body(alert));
    }

    @GetMapping
    @Operation(summary = "All alerts, optionally only ACTIVE or TRIGGERED ones")
    Flux<PriceAlert> list(@RequestParam(required = false) PriceAlert.@Nullable Status status) {
        return repository.findAll(status);
    }

    @DeleteMapping("/{id}")
    @Operation(summary = "Delete an alert")
    Mono<ResponseEntity<Void>> delete(@PathVariable long id) {
        return repository.delete(id)
                .flatMap(deleted -> deleted
                        ? refreshEngine().thenReturn(ResponseEntity.noContent().<Void>build())
                        : Mono.error(new ResponseStatusException(HttpStatus.NOT_FOUND, "Alert " + id + " not found")));
    }

    @GetMapping(path = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    @Operation(summary = "Alerts as they fire (Server-Sent Events)")
    Flux<ServerSentEvent<PriceAlert>> stream() {
        return ServerSentEvents.stream("alert", events.triggered());
    }

    /**
     * Only the instance that generates prices evaluates alerts; others rely on its reload.
     */
    private Mono<Void> refreshEngine() {
        AlertEngine local = engine.getIfAvailable();
        return local == null ? Mono.empty() : local.refresh();
    }

    record NewAlert(
            @NotBlank String symbol,
            @NotNull PriceAlert.Direction direction,
            @NotNull @Positive @Digits(integer = 12, fraction = 6) BigDecimal price) {
    }
}
