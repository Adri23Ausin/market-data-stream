CREATE TABLE candles
(
    symbol           VARCHAR(20)    NOT NULL,
    interval_seconds INT            NOT NULL,
    open_time        TIMESTAMPTZ    NOT NULL,
    open             NUMERIC(18, 6) NOT NULL,
    high             NUMERIC(18, 6) NOT NULL,
    low              NUMERIC(18, 6) NOT NULL,
    close            NUMERIC(18, 6) NOT NULL,
    volume           BIGINT         NOT NULL,
    trades           INT            NOT NULL,
    -- The natural key doubles as the index for "candles of a symbol between two dates"
    PRIMARY KEY (symbol, interval_seconds, open_time),
    CONSTRAINT ck_candles_range CHECK (low <= open AND low <= close AND high >= open AND high >= close)
);
