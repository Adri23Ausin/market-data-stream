package com.adrianausin.marketdata.alerts;

import java.math.BigDecimal;
import java.time.Instant;

import org.jspecify.annotations.Nullable;

public record PriceAlert(
        long id,
        String symbol,
        Direction direction,
        BigDecimal targetPrice,
        Status status,
        Instant createdAt,
        @Nullable Instant triggeredAt,
        @Nullable BigDecimal triggeredPrice) {

    public enum Direction {
        /** Fires when the price reaches or goes above the target. */
        ABOVE,
        /** Fires when the price reaches or goes below the target. */
        BELOW
    }

    public enum Status {
        ACTIVE, TRIGGERED
    }

    public boolean isMetBy(BigDecimal price) {
        int comparison = price.compareTo(targetPrice);
        return switch (direction) {
            case ABOVE -> comparison >= 0;
            case BELOW -> comparison <= 0;
        };
    }
}
