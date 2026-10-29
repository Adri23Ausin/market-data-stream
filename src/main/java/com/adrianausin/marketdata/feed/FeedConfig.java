package com.adrianausin.marketdata.feed;

import org.springframework.boot.autoconfigure.condition.ConditionalOnBooleanProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class FeedConfig {

    /**
     * Replicas ({@code market.feed.enabled=false}, distributed mode) only stream prices that
     * another instance publishes, so they don't run a simulator at all.
     */
    @Bean
    @ConditionalOnBooleanProperty(name = "market.feed.enabled", matchIfMissing = true)
    SimulatedMarketFeed simulatedMarketFeed(MarketProperties properties) {
        return new SimulatedMarketFeed(properties);
    }
}
