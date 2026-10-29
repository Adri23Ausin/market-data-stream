package com.adrianausin.marketdata.feed;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
class FeedConfig {

    @Bean
    SimulatedMarketFeed simulatedMarketFeed(MarketProperties properties) {
        return new SimulatedMarketFeed(properties);
    }
}
