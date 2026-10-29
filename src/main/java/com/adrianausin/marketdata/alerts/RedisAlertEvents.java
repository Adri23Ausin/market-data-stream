package com.adrianausin.marketdata.alerts;

import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

class RedisAlertEvents implements AlertEvents {

    static final String CHANNEL = "market:alerts";

    private final ReactiveStringRedisTemplate redis;
    private final Flux<PriceAlert> triggered;
    private final JsonMapper jsonMapper;

    RedisAlertEvents(ReactiveStringRedisTemplate redis, ReactiveRedisMessageListenerContainer container,
                     JsonMapper jsonMapper) {
        this.redis = redis;
        this.jsonMapper = jsonMapper;
        // One Redis subscription per instance, shared by all its SSE clients
        this.triggered = container.receive(ChannelTopic.of(CHANNEL))
                .map(message -> jsonMapper.readValue(message.getMessage(), PriceAlert.class))
                .share();
    }

    @Override
    public Mono<Void> publish(PriceAlert alert) {
        return redis.convertAndSend(CHANNEL, jsonMapper.writeValueAsString(alert)).then();
    }

    @Override
    public Flux<PriceAlert> triggered() {
        return triggered;
    }
}
