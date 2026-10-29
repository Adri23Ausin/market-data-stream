CREATE TABLE price_alerts
(
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    symbol          VARCHAR(20)    NOT NULL,
    direction       VARCHAR(5)     NOT NULL,
    target_price    NUMERIC(18, 6) NOT NULL,
    status          VARCHAR(10)    NOT NULL DEFAULT 'ACTIVE',
    created_at      TIMESTAMPTZ    NOT NULL DEFAULT now(),
    triggered_at    TIMESTAMPTZ,
    triggered_price NUMERIC(18, 6),
    CONSTRAINT ck_price_alerts_direction CHECK (direction IN ('ABOVE', 'BELOW')),
    CONSTRAINT ck_price_alerts_status CHECK (status IN ('ACTIVE', 'TRIGGERED')),
    CONSTRAINT ck_price_alerts_target CHECK (target_price > 0)
);

CREATE INDEX idx_price_alerts_active ON price_alerts (symbol) WHERE status = 'ACTIVE';
