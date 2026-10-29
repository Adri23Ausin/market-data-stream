package com.adrianausin.marketdata.feed;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import com.adrianausin.marketdata.feed.MarketProperties.Instrument;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;

/**
 * Generates a tick per instrument at every interval, moving prices with a geometric Brownian
 * motion. Cold and deterministic for a given seed and scheduler, which makes it easy to test
 * with virtual time.
 */
public class PriceSimulator {

    private final List<Instrument> instruments;
    private final Duration interval;
    private final Scheduler scheduler;
    private final Random random;

    public PriceSimulator(List<Instrument> instruments, Duration interval, Scheduler scheduler, Random random) {
        this.instruments = List.copyOf(instruments);
        this.interval = interval;
        this.scheduler = scheduler;
        this.random = random;
    }

    public Flux<Tick> ticks() {
        return Flux.defer(() -> {
            // State per subscription, only touched by the interval's single worker thread
            double[] prices = instruments.stream().mapToDouble(i -> i.initialPrice().doubleValue()).toArray();
            double dtYears = interval.toMillis() / 1000.0 / GeometricBrownianMotion.SECONDS_PER_TRADING_YEAR;
            return Flux.interval(interval, scheduler)
                    .concatMapIterable(round -> nextRound(prices, dtYears));
        });
    }

    private List<Tick> nextRound(double[] prices, double dtYears) {
        Instant now = Instant.ofEpochMilli(scheduler.now(TimeUnit.MILLISECONDS));
        List<Tick> ticks = new ArrayList<>(instruments.size());
        for (int i = 0; i < instruments.size(); i++) {
            Instrument instrument = instruments.get(i);
            prices[i] = GeometricBrownianMotion.next(prices[i], instrument.drift(), instrument.volatility(), dtYears,
                    random.nextGaussian());
            BigDecimal price = BigDecimal.valueOf(prices[i]).setScale(instrument.decimals(), RoundingMode.HALF_EVEN);
            long volume = 1 + (long) (random.nextDouble() * 500);
            ticks.add(new Tick(instrument.symbol(), price, volume, now));
        }
        return ticks;
    }
}
