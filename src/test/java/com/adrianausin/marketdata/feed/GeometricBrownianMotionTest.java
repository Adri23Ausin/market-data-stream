package com.adrianausin.marketdata.feed;

import java.util.Random;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class GeometricBrownianMotionTest {

    @Test
    void withoutVolatilityThePriceGrowsAtTheDriftRate() {
        double afterOneYear = GeometricBrownianMotion.next(100, 0.10, 0, 1, 0.7);

        assertThat(afterOneYear).isCloseTo(100 * Math.exp(0.10), within(1e-9));
    }

    @Test
    void pricesNeverGoNegative() {
        assertThat(GeometricBrownianMotion.next(1, 0, 2.0, 1, -10)).isPositive();
    }

    @Test
    void logReturnsHaveTheExpectedMeanAndVolatility() {
        Random random = new Random(42);
        double drift = 0.08;
        double volatility = 0.30;
        double dt = 1.0 / 252;
        int days = 200_000;

        double sum = 0;
        double sumOfSquares = 0;
        for (int i = 0; i < days; i++) {
            double logReturn = Math.log(GeometricBrownianMotion.next(100, drift, volatility, dt, random.nextGaussian()) / 100);
            sum += logReturn;
            sumOfSquares += logReturn * logReturn;
        }
        double mean = sum / days;
        double stdDev = Math.sqrt(sumOfSquares / days - mean * mean);

        assertThat(mean / dt).as("annualised mean log return").isCloseTo(drift - volatility * volatility / 2, within(0.02));
        assertThat(stdDev / Math.sqrt(dt)).as("annualised volatility").isCloseTo(volatility, within(0.005));
    }
}
