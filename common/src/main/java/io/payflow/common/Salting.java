package io.payflow.common;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.utils.Utils;

/** Stage-1 rollup key: merchant_id#(murmur2(payment_id) % B). */
public final class Salting {
    private Salting() {
    }

    public static int bucket(String paymentId, int buckets) {
        return Utils.toPositive(Utils.murmur2(paymentId.getBytes(StandardCharsets.UTF_8))) % buckets;
    }

    public static String stage1Key(String merchantId, String paymentId, int buckets) {
        return merchantId + "#" + bucket(paymentId, buckets);
    }

    public static String merchantOf(String stage1Key) {
        int i = stage1Key.indexOf('#');
        return i < 0 ? stage1Key : stage1Key.substring(0, i);
    }
}
