package com.adrianausin.marketdata.alerts;

import java.math.BigDecimal;
import java.time.Instant;

import com.adrianausin.marketdata.alerts.PriceAlert.Direction;
import com.adrianausin.marketdata.alerts.PriceAlert.Status;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class PriceAlertTest {

    @ParameterizedTest
    @CsvSource({
            "ABOVE, 150.00, 149.99, false",
            "ABOVE, 150.00, 150.00, true",
            "ABOVE, 150.00, 150.01, true",
            "BELOW, 150.00, 150.01, false",
            "BELOW, 150.00, 150,    true",
            "BELOW, 150.00, 149.50, true"
    })
    void firesWhenThePriceReachesTheTarget(Direction direction, BigDecimal target, BigDecimal price, boolean fires) {
        PriceAlert alert = new PriceAlert(1, "NVDA", direction, target, Status.ACTIVE, Instant.EPOCH, null, null);

        assertThat(alert.isMetBy(price)).isEqualTo(fires);
    }
}
