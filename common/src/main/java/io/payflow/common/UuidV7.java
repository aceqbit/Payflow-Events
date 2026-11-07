package io.payflow.common;

import java.security.SecureRandom;
import java.util.UUID;

/** RFC 9562 UUIDv7 generator for apps that must know an event id before any DB round trip (dual-write baselines). */
public final class UuidV7 {
    private static final SecureRandom RANDOM = new SecureRandom();

    private UuidV7() {
    }

    public static UUID next() {
        return next(System.currentTimeMillis());
    }

    static UUID next(long unixMs) {
        long rand = RANDOM.nextLong();
        long msb = (unixMs << 16) | 0x7000L | ((rand >>> 52) & 0x0FFFL);
        long lsb = (RANDOM.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb);
    }
}
