package io.payflow.common;

/** Deserialization or validation failure: goes straight to the DLQ (rule 7). */
public class NonRetryableEventException extends RuntimeException {
    public NonRetryableEventException(String message) {
        super(message);
    }
}
