package com.adrianausin.marketdata.alerts;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import tools.jackson.databind.json.JsonMapper;

@Configuration(proxyBeanMethods = false)
class AlertConfig {

    @Bean
    @ConditionalOnProperty(name = "market.distribution", havingValue = "redis")
    AlertEvents redisAlertEvents(ReactiveStringRedisTemplate redis, ReactiveRedisMessageListenerContainer container,
                                 JsonMapper jsonMapper) {
        return new RedisAlertEvents(redis, container, jsonMapper);
    }

    @Bean
    @ConditionalOnProperty(name = "market.distribution", havingValue = "local", matchIfMissing = true)
    AlertEvents localAlertEvents() {
        return new LocalAlertEvents();
    }
}
