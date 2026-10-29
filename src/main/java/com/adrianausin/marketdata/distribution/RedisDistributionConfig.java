package com.adrianausin.marketdata.distribution;

import com.adrianausin.marketdata.feed.SimulatedMarketFeed;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import tools.jackson.databind.json.JsonMapper;

/**
 * {@code market.distribution=redis}: one instance generates prices and publishes them to Redis,
 * every instance (including that one) streams from Redis. Instances can then be scaled
 * horizontally behind a load balancer and all their clients see the same prices.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "market.distribution", havingValue = "redis")
class RedisDistributionConfig {

    @Bean
    ReactiveRedisMessageListenerContainer redisMessageListenerContainer(ReactiveRedisConnectionFactory factory) {
        return new ReactiveRedisMessageListenerContainer(factory);
    }

    @Bean
    @Primary
    RedisMarketFeed redisMarketFeed(ReactiveRedisMessageListenerContainer container, JsonMapper jsonMapper) {
        return new RedisMarketFeed(container, jsonMapper);
    }

    @Bean
    @ConditionalOnBooleanProperty(name = "market.feed.enabled", matchIfMissing = true)
    RedisTickPublisher redisTickPublisher(SimulatedMarketFeed simulator, ReactiveStringRedisTemplate redis,
                                          JsonMapper jsonMapper) {
        return new RedisTickPublisher(simulator, redis, jsonMapper);
    }

    @Bean
    RedisPriceSnapshot redisPriceSnapshot(ReactiveStringRedisTemplate redis, JsonMapper jsonMapper) {
        return new RedisPriceSnapshot(redis, jsonMapper);
    }
}
