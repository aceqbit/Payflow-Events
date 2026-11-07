package io.payflow.common;

import io.micrometer.tracing.TraceContext;

/** W3C traceparent for the outbox column / Kafka header, taken from the current span. */
public final class Traceparent {
    private Traceparent() {
    }

    public static String of(TraceContext ctx) {
        return format(ctx.traceId(), ctx.spanId(), Boolean.TRUE.equals(ctx.sampled()));
    }

    static String format(String traceId, String spanId, boolean sampled) {
        return "00-" + pad(traceId, 32) + "-" + pad(spanId, 16) + "-" + (sampled ? "01" : "00");
    }

    private static String pad(String hex, int len) {
        return hex.length() >= len ? hex : "0".repeat(len - hex.length()) + hex;
    }
}
