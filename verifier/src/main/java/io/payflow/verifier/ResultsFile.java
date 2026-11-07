package io.payflow.verifier;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/** docs/results.json: one row per verified run. report.py reads it; README numbers must come from it. */
public final class ResultsFile {
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    private ResultsFile() {
    }

    public static synchronized void append(Path file, String runId, String scenario, String relay,
                                           Verification.Result r, Map<String, Object> extra) throws IOException {
        List<Map<String, Object>> rows = Files.exists(file) && Files.size(file) > 0
                ? MAPPER.readValue(Files.readString(file, StandardCharsets.UTF_8), new TypeReference<List<Map<String, Object>>>() { })
                : new ArrayList<>();
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("run_id", runId);
        row.put("scenario", scenario);
        row.put("relay", relay);
        row.put("verified_at", Instant.now().toString());
        row.put("committed", r.committed());
        row.put("lost", r.lost());
        row.put("phantom", r.phantom());
        row.put("duplicate_effects", r.duplicateEffects());
        row.put("duplicates_received", r.duplicatesReceived());
        row.put("order_violations", r.orderViolations());
        row.put("dlq_count", r.dlqCount());
        row.put("pills_injected", r.pillsInjected());
        row.put("rollup_mismatch_minor", r.rollupMismatchMinor());
        row.put("outbox_clean", r.outboxClean());
        row.putAll(extra);
        rows.removeIf(x -> runId.equals(x.get("run_id")));
        rows.add(row);
        Path parent = file.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Files.writeString(file, MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(rows), StandardCharsets.UTF_8);
    }
}
