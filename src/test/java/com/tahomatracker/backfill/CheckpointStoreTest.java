package com.tahomatracker.backfill;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CheckpointStoreTest {

    @Test
    void saveOverwritesExistingFile() throws Exception {
        Path dir = Files.createTempDirectory("cp-test");
        Path cp = dir.resolve("backfill-checkpoint.json");
        CheckpointStore cs = new CheckpointStore(cp);

        Map<String, Map<String, Object>> m1 = new HashMap<>();
        var e1 = new HashMap<String, Object>();
        e1.put("status", "ok");
        m1.put("t1", e1);
        cs.save(m1);
        var loaded1 = cs.load();
        assertEquals("ok", loaded1.get("t1").get("status").toString());

        Map<String, Map<String, Object>> m2 = new HashMap<>();
        var e2 = new HashMap<String, Object>();
        e2.put("status", "error");
        m2.put("t2", e2);
        cs.save(m2);
        var loaded2 = cs.load();
        assertEquals("error", loaded2.get("t2").get("status").toString());
    }
}
