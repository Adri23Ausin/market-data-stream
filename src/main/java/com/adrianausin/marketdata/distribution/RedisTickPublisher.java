package com.adrianausin.marketdata.distribution;

import com.adrianausin.marketdata.feed.SimulatedMarketFeed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.Disposable;
import tools.jackson.databind.json.JsonMapper;

/**
 * Runs on the instance that generates prices: publishes each tick on the Redis channel and
 * keeps the latest price of every symbol in a hash, so instances that start later (or
 * restart) can serve prices straight away.
 */
public class RedisTickPublisher implements SmartLifecycle {

    private static final Logger log = LoggerFactory.getLogger(RedisTickPublisher.class);

    private final SimulatedMarketFeed simulator;
    private final ReactiveStringRedisTemplate redis;
    private final JsonMapper jsonMapper;
    private volatile Disposable subscription;

    public RedisTickPublisher(SimulatedMarketFeed simulator, ReactiveStringRedisTemplate redis, JsonMapper jsonMapper) {
        this.simulator = simulator;
        this.redis = redis;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public void start() {
        subscription = simulator.ticks()
                .map(tick -> new Published(tick.symbol(), jsonMapper.writeValueAsString(tick)))
                // Publish in order, one at a time: concatMap keeps the ticks of a symbol ordered
                .concatMap(tick -> redis.opsForHash().put(RedisChannels.LATEST, tick.symbol(), tick.json())
                        .then(redis.convertAndSend(RedisChannels.TICKS, tick.json())))
                .onErrorContinue((error, tick) -> log.warn("Could not publish tick to Redis: {}", error.toString()))
                .subscribe();
        log.info("Publishing simulated ticks to Redis");
    }

    @Override
    public void stop() {
        if (subscription != null) {
            subscription.dispose();
        }
    }

    @Override
    public boolean isRunning() {
        return subscription != null && !subscription.isDisposed();
    }

    /**
     * A lower phase starts earlier: subscribe before the simulator starts, so its first ticks
     * are published too.
     */
    @Override
    public int getPhase() {
        return SmartLifecycle.DEFAULT_PHASE - 1;
    }

    private record Published(String symbol, String json) {
    }
}
