package com.adrianausin.marketdata.distribution;

import com.adrianausin.marketdata.feed.MarketFeed;
import com.adrianausin.marketdata.feed.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import tools.jackson.databind.json.JsonMapper;

/**
 * The feed every instance consumes when running distributed: ticks published on a Redis
 * channel by the instance that generates them. Each instance keeps a single Redis
 * subscription and fans it out to all of its own clients.
 */
public class RedisMarketFeed implements MarketFeed, SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RedisMarketFeed.class);

    private final ReactiveRedisMessageListenerContainer container;
    private final JsonMapper jsonMapper;
    private final Sinks.Many<Tick> sink = Sinks.many().multicast().directBestEffort();
    private volatile Disposable subscription;

    public RedisMarketFeed(ReactiveRedisMessageListenerContainer container, JsonMapper jsonMapper) {
        this.container = container;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public Flux<Tick> ticks() {
        return MarketFeed.buffered(sink.asFlux());
    }

    @Override
    public void start() {
        subscription = container.receive(ChannelTopic.of(RedisChannels.TICKS))
                .map(message -> jsonMapper.readValue(message.getMessage(), Tick.class))
                .subscribe(sink::tryEmitNext, error -> log.error("Lost the Redis tick subscription", error));
        log.info("Consuming ticks from Redis channel {}", RedisChannels.TICKS);
    }

    @Override
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
        sink.tryEmitComplete();
    }

    @Override
    public boolean isRunning() {
        return subscription != null && !subscription.isDisposed();
    }
}
