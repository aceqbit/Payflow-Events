package io.payflow.source;

/** retain: outbox rows stay (polling relay marks published_at). delete: row deleted in the same tx (CDC reads the WAL). */
public enum OutboxMode {
    RETAIN, DELETE;

    public static OutboxMode parse(String s) {
        return switch (s.toLowerCase()) {
            case "retain" -> RETAIN;
            case "delete" -> DELETE;
            default -> throw new IllegalArgumentException("outbox.mode must be retain|delete, got " + s);
        };
    }
}
