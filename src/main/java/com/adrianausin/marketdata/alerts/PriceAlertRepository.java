package com.adrianausin.marketdata.alerts;

import java.math.BigDecimal;
import java.time.Instant;

import com.adrianausin.marketdata.alerts.PriceAlert.Direction;
import com.adrianausin.marketdata.alerts.PriceAlert.Status;
import io.r2dbc.spi.Readable;
import org.jspecify.annotations.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class PriceAlertRepository {

    private static final String COLUMNS =
            "id, symbol, direction, target_price, status, created_at, triggered_at, triggered_price";

    private final DatabaseClient db;

    public PriceAlertRepository(DatabaseClient db) {
        this.db = db;
    }

    public Mono<PriceAlert> create(String symbol, Direction direction, BigDecimal targetPrice) {
        return db.sql("INSERT INTO price_alerts (symbol, direction, target_price) VALUES (:symbol, :direction, :target) "
                        + "RETURNING " + COLUMNS)
                .bind("symbol", symbol)
                .bind("direction", direction.name())
                .bind("target", targetPrice)
                .map(PriceAlertRepository::toAlert)
                .one();
    }

    public Flux<PriceAlert> findAll(@Nullable Status status) {
        var spec = status == null
                ? db.sql("SELECT " + COLUMNS + " FROM price_alerts ORDER BY id")
                : db.sql("SELECT " + COLUMNS + " FROM price_alerts WHERE status = :status ORDER BY id")
                        .bind("status", status.name());
        return spec.map(PriceAlertRepository::toAlert).all();
    }

    public Mono<Boolean> delete(long id) {
        return db.sql("DELETE FROM price_alerts WHERE id = :id").bind("id", id)
                .fetch().rowsUpdated().map(rows -> rows > 0);
    }

    /**
     * Marks an alert as triggered unless it already was. The {@code status = 'ACTIVE'} condition
     * makes this safe when several ticks (or instances) try to fire the same alert: exactly one
     * of them gets the row back.
     */
    public Mono<PriceAlert> trigger(long id, BigDecimal price, Instant at) {
        return db.sql("UPDATE price_alerts SET status = 'TRIGGERED', triggered_at = :at, triggered_price = :price "
                        + "WHERE id = :id AND status = 'ACTIVE' RETURNING " + COLUMNS)
                .bind("id", id)
                .bind("price", price)
                .bind("at", at)
                .map(PriceAlertRepository::toAlert)
                .one();
    }

    private static PriceAlert toAlert(Readable row) {
        return new PriceAlert(row.get("id", Long.class), row.get("symbol", String.class),
                Direction.valueOf(row.get("direction", String.class)), row.get("target_price", BigDecimal.class),
                Status.valueOf(row.get("status", String.class)), row.get("created_at", Instant.class),
                row.get("triggered_at", Instant.class), row.get("triggered_price", BigDecimal.class));
    }
}
