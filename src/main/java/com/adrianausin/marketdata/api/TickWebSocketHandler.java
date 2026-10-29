package com.adrianausin.marketdata.api;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import com.adrianausin.marketdata.feed.MarketFeed;
import com.adrianausin.marketdata.feed.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.socket.WebSocketHandler;
import org.springframework.web.reactive.socket.WebSocketMessage;
import org.springframework.web.reactive.socket.WebSocketSession;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

/**
 * WebSocket at {@code /ws/ticks}, for clients that change their subscriptions on the fly:
 * <pre>
 * → {"action": "subscribe", "symbols": ["AAPL", "NVDA"]}
 * → {"action": "unsubscribe", "symbols": ["AAPL"]}
 * ← {"symbol": "NVDA", "price": 138.92, "volume": 120, "timestamp": "..."}
 * </pre>
 * Every new subscription set {@code switchMap}s to a freshly filtered feed, so the change takes
 * effect on the very next tick without reconnecting.
 */
@Component
class TickWebSocketHandler implements WebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(TickWebSocketHandler.class);

    private final MarketFeed feed;
    private final JsonMapper jsonMapper;

    TickWebSocketHandler(MarketFeed feed, JsonMapper jsonMapper) {
        this.feed = feed;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public Mono<Void> handle(WebSocketSession session) {
        Flux<Set<String>> subscriptions = session.receive()
                .map(WebSocketMessage::getPayloadAsText)
                .flatMap(this::parse)
                .scan(Set.<String>of(), TickWebSocketHandler::apply)
                .distinctUntilChanged();

        Flux<WebSocketMessage> outbound = subscriptions
                .switchMap(symbols -> symbols.isEmpty()
                        ? Flux.<Tick>empty()
                        : feed.ticks().filter(tick -> symbols.contains(tick.symbol())))
                .onBackpressureLatest()
                .map(tick -> session.textMessage(jsonMapper.writeValueAsString(tick)));

        return session.send(outbound);
    }

    private Mono<Command> parse(String payload) {
        try {
            return Mono.just(jsonMapper.readValue(payload, Command.class));
        } catch (RuntimeException e) {
            // A malformed message is ignored rather than closing the client's connection
            log.debug("Ignoring invalid WebSocket message: {}", payload);
            return Mono.empty();
        }
    }

    private static Set<String> apply(Set<String> current, Command command) {
        Set<String> next = new HashSet<>(current);
        List<String> symbols = command.symbols() == null ? List.of() : command.symbols();
        symbols.stream().map(s -> s.toUpperCase(Locale.ROOT)).forEach(symbol -> {
            if ("subscribe".equalsIgnoreCase(command.action())) {
                next.add(symbol);
            } else if ("unsubscribe".equalsIgnoreCase(command.action())) {
                next.remove(symbol);
            }
        });
        return Set.copyOf(next);
    }

    record Command(String action, List<String> symbols) {
    }
}
