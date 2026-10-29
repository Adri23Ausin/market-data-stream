package com.adrianausin.marketdata.feed;

/**
 * The standard model for stock prices (the one behind Black-Scholes): log returns are normally
 * distributed, so prices stay positive and move proportionally to their level.
 * <pre>
 * S(t + dt) = S(t) * exp((mu - sigma^2 / 2) * dt + sigma * sqrt(dt) * Z),   Z ~ N(0, 1)
 * </pre>
 */
final class GeometricBrownianMotion {

    /** Seconds in a trading year (252 days of 6.5 hours), to express dt in years. */
    static final double SECONDS_PER_TRADING_YEAR = 252 * 6.5 * 3600;

    private GeometricBrownianMotion() {
    }

    /**
     * @param dtYears  time step in years
     * @param gaussian a sample of the standard normal distribution
     */
    static double next(double price, double drift, double volatility, double dtYears, double gaussian) {
        double exponent = (drift - volatility * volatility / 2) * dtYears
                + volatility * Math.sqrt(dtYears) * gaussian;
        return price * Math.exp(exponent);
    }
}
