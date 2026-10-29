package com.adrianausin.marketdata.api;

import java.util.Map;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.HandlerMapping;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;

@Configuration(proxyBeanMethods = false)
class WebSocketConfig {

    @Bean
    HandlerMapping webSocketMapping(TickWebSocketHandler tickHandler) {
        // Order -1: checked before the annotated controllers
        return new SimpleUrlHandlerMapping(Map.of("/ws/ticks", tickHandler), -1);
    }
}
