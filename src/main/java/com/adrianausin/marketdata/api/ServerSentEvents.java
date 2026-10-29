package com.adrianausin.marketdata.api;

import java.time.Duration;

import org.springframework.http.codec.ServerSentEvent;
import reactor.core.publisher.Flux;

/**
 * Shared shape of every SSE endpoint:
 * <ul>
 *     <li>a {@code connected} comment straight away, so the response headers are sent even if the
 *     first real event takes a while (an alert may never fire);</li>
 *     <li>a {@code heartbeat} comment every {@value #HEARTBEAT_SECONDS} seconds, so proxies and load
 *     balancers don't close connections that look idle.</li>
 * </ul>
 */
final class ServerSentEvents {

    static final long HEARTBEAT_SECONDS = 15;

    private ServerSentEvents() {
    }

    static <T> Flux<ServerSentEvent<T>> stream(String eventName, Flux<T> data) {
        Flux<ServerSentEvent<T>> events = data.map(item -> ServerSentEvent.builder(item).event(eventName).build());
        Flux<ServerSentEvent<T>> heartbeat = Flux.interval(Duration.ofSeconds(HEARTBEAT_SECONDS))
                .map(i -> ServerSentEvent.<T>builder().comment("heartbeat").build());
        return Flux.merge(events, heartbeat)
                .startWith(ServerSentEvent.<T>builder().comment("connected").build());
    }
}
