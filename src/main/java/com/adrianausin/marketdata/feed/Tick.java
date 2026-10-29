package com.adrianausin.marketdata.feed;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * A trade: {@code volume} units of {@code symbol} changed hands at {@code price}.
 */
public record Tick(String symbol, BigDecimal price, long volume, Instant timestamp) {
}
