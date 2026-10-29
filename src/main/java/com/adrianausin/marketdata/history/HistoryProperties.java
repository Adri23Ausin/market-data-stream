package com.adrianausin.marketdata.history;

import java.time.Duration;
import java.util.List;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * @param intervals candle intervals to store; each must be one of the live candle intervals
 */
@ConfigurationProperties("market.history")
public record HistoryProperties(@DefaultValue("1m") List<Duration> intervals) {
}
