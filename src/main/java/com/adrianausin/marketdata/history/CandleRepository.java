package com.adrianausin.marketdata.history;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;

import com.adrianausin.marketdata.candles.Candle;
import io.r2dbc.spi.Readable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Non-blocking access to stored candles with R2DBC, so the database never ties up one of the
 * few event-loop threads WebFlux runs on.
 */
@Repository
public class CandleRepository {

    private final DatabaseClient db;

    public CandleRepository(DatabaseClient db) {
        this.db = db;
    }

    /**
     * Stores candles in one round trip. A candle that already exists is kept as is: candles
     * are only written once their interval has closed, so a duplicate can only be a replay.
     */
    public Mono<Long> saveAll(List<Candle> candles) {
        if (candles.isEmpty()) {
            return Mono.just(0L);
        }
        return db.inConnectionMany(connection -> {
                    var statement = connection.createStatement("""
                            INSERT INTO candles (symbol, interval_seconds, open_time, open, high, low, close, volume, trades)
                            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT (symbol, interval_seconds, open_time) DO NOTHING
                            """);
                    for (int i = 0; i < candles.size(); i++) {
                        Candle candle = candles.get(i);
                        statement.bind(0, candle.symbol())
                                .bind(1, (int) candle.intervalSeconds())
                                .bind(2, candle.openTime())
                                .bind(3, candle.open())
                                .bind(4, candle.high())
                                .bind(5, candle.low())
                                .bind(6, candle.close())
                                .bind(7, candle.volume())
                                .bind(8, candle.trades());
                        if (i < candles.size() - 1) {
                            statement.add();
                        }
                    }
                    return Flux.from(statement.execute()).flatMap(result -> Mono.from(result.getRowsUpdated()));
                })
                .reduce(0L, Long::sum);
    }

    /**
     * The most recent {@code limit} candles in {@code [from, to)}, oldest first.
     */
    public Flux<Candle> find(String symbol, long intervalSeconds, Instant from, Instant to, int limit) {
        return db.sql("""
                        SELECT * FROM (
                            SELECT * FROM candles
                            WHERE symbol = :symbol AND interval_seconds = :interval
                              AND open_time >= :from AND open_time < :to
                            ORDER BY open_time DESC
                            LIMIT :limit
                        ) latest
                        ORDER BY open_time
                        """)
                .bind("symbol", symbol)
                .bind("interval", (int) intervalSeconds)
                .bind("from", from)
                .bind("to", to)
                .bind("limit", limit)
                .map(CandleRepository::toCandle)
                .all();
    }

    private static Candle toCandle(Readable row) {
        return new Candle(row.get("symbol", String.class), row.get("open_time", Instant.class),
                row.get("interval_seconds", Integer.class), row.get("open", BigDecimal.class),
                row.get("high", BigDecimal.class), row.get("low", BigDecimal.class),
                row.get("close", BigDecimal.class), row.get("volume", Long.class),
                row.get("trades", Integer.class));
    }
}
