package com.tahomatracker.backfill;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class CheckpointStore {
    private final Path path;
    private final ObjectMapper mapper = new ObjectMapper();

    public CheckpointStore(Path path) {
        this.path = path;
    }

    public synchronized Map<String, Map<String, Object>> load() {
        if (!Files.exists(path)) {
            return new HashMap<>();
        }
        try {
            return mapper.readValue(Files.readAllBytes(path), new TypeReference<>() {});
        } catch (IOException e) {
            throw new RuntimeException("Failed to read checkpoint file", e);
        }
    }

    public synchronized void save(Map<String, Map<String, Object>> state) {
        try {
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
            Path parent = path.getParent() != null ? path.getParent() : Path.of(".");
            Path tmp = Files.createTempFile(parent, path.getFileName().toString() + "-", ".tmp");
            Files.write(tmp, bytes);
            try {
                Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write checkpoint file", e);
        }
    }

    public void markOk(String ts) {
        var s = load();
        var entry = new HashMap<String, Object>();
        entry.put("status", "ok");
        entry.put("updated_at", Instant.now().toString());
        s.put(ts, entry);
        save(s);
    }

    public void markError(String ts, String error) {
        var s = load();
        var entry = new HashMap<String, Object>();
        entry.put("status", "error");
        entry.put("error", error);
        entry.put("updated_at", Instant.now().toString());
        s.put(ts, entry);
        save(s);
    }
}
