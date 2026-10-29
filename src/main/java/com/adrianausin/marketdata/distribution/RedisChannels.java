package com.adrianausin.marketdata.distribution;

public final class RedisChannels {

    /** Pub/sub channel carrying every tick as JSON. */
    public static final String TICKS = "market:ticks";

    /** Hash of symbol to the JSON of its latest tick. */
    public static final String LATEST = "market:latest";

    private RedisChannels() {
    }
}
