package io.payflow.common;

/** Kafka header names of the record shape (plan section 3). */
public enum Headers {
    ID("id"), TYPE("type"), SEQ("seq"), OCCURRED_AT("occurred_at"), TRACEPARENT("traceparent"), MERCHANT_ID("merchant_id");

    private final String key;

    Headers(String key) {
        this.key = key;
    }

    public String key() {
        return key;
    }
}
