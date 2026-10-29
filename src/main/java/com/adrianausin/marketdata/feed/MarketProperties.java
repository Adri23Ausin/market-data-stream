package com.adrianausin.marketdata.feed;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.PositiveOrZero;
import org.jspecify.annotations.Nullable;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("market")
public record MarketProperties(@Valid Feed feed, @NotEmpty @Valid List<Instrument> instruments) {

    /**
     * @param enabled      whether this instance generates prices (only one instance should)
     * @param tickInterval time between two rounds of ticks
     * @param seed         fixed random seed for reproducible prices, random when absent
     */
    public record Feed(
            @DefaultValue("true") boolean enabled,
            @DefaultValue("250ms") Duration tickInterval,
            @Nullable Long seed) {
    }

    /**
     * @param drift      expected annual return (0.08 = 8%)
     * @param volatility annualised volatility (0.30 = 30%)
     * @param decimals   price precision
     */
    public record Instrument(
            @NotBlank String symbol,
            @NotBlank String name,
            @Positive BigDecimal initialPrice,
            double drift,
            @PositiveOrZero double volatility,
            @DefaultValue("2") @PositiveOrZero int decimals) {
    }
}
