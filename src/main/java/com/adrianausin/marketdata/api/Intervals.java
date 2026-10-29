package com.adrianausin.marketdata.api;

import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses candle intervals written the way traders do: {@code 5s}, {@code 1m}, {@code 1h}.
 */
final class Intervals {

    private static final Pattern FORMAT = Pattern.compile("(\\d+)([smh])");

    private Intervals() {
    }

    static Duration parse(String value) {
        Matcher matcher = FORMAT.matcher(value.strip().toLowerCase());
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Invalid interval '%s', expected e.g. 5s, 1m or 1h".formatted(value));
        }
        long amount = Long.parseLong(matcher.group(1));
        return switch (matcher.group(2)) {
            case "s" -> Duration.ofSeconds(amount);
            case "m" -> Duration.ofMinutes(amount);
            default -> Duration.ofHours(amount);
        };
    }
}
