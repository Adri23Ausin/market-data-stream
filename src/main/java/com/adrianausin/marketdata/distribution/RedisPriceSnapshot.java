package com.adrianausin.marketdata.distribution;

import com.adrianausin.marketdata.feed.PriceSnapshot;
import com.adrianausin.marketdata.feed.Tick;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import reactor.core.publisher.Flux;
import tools.jackson.databind.json.JsonMapper;

class RedisPriceSnapshot implements PriceSnapshot {

    private final ReactiveStringRedisTemplate redis;
    private final JsonMapper jsonMapper;

    RedisPriceSnapshot(ReactiveStringRedisTemplate redis, JsonMapper jsonMapper) {
        this.redis = redis;
        this.jsonMapper = jsonMapper;
    }

    @Override
    public Flux<Tick> latest() {
        return redis.<String, String>opsForHash().values(RedisChannels.LATEST)
                .map(json -> jsonMapper.readValue(json, Tick.class));
    }
}
