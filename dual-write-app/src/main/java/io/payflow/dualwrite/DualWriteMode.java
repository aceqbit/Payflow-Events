package io.payflow.dualwrite;

/**
 * DB_FIRST: commit, then send (a crash or send failure in between loses the event).
 * KAFKA_FIRST: send and wait for the ack, then commit (a rollback or crash after the ack leaves a phantom event).
 */
public enum DualWriteMode {
    DB_FIRST, KAFKA_FIRST;

    public static DualWriteMode parse(String s) {
        return switch (s.toLowerCase()) {
            case "db-first" -> DB_FIRST;
            case "kafka-first" -> KAFKA_FIRST;
            default -> throw new IllegalArgumentException("dualwrite.mode must be db-first|kafka-first, got " + s);
        };
    }
}
