package com.adrianausin.marketdata.candles;

import java.math.BigDecimal;
import java.time.Instant;

import com.adrianausin.marketdata.feed.Tick;

/**
 * OHLC candle: open, high, low and close prices of a symbol over one interval starting at
 * {@code openTime}, plus the traded volume and number of trades.
 */
public record Candle(
        String symbol,
        Instant openTime,
        long intervalSeconds,
        BigDecimal open,
        BigDecimal high,
        BigDecimal low,
        BigDecimal close,
        long volume,
        int trades) {

    /**
     * The candle of a single trade.
     */
    static Candle of(Tick tick, Instant openTime, long intervalSeconds) {
        return new Candle(tick.symbol(), openTime, intervalSeconds, tick.price(), tick.price(), tick.price(),
                tick.price(), tick.volume(), 1);
    }

    /**
     * Merges this candle with the one that immediately follows it in the same interval.
     */
    Candle merge(Candle next) {
        return new Candle(symbol, openTime, intervalSeconds, open, high.max(next.high), low.min(next.low),
                next.close, volume + next.volume, trades + next.trades);
    }
}
