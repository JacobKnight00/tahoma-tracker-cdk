package com.tahomatracker.service.scraper;

import static org.junit.jupiter.api.Assertions.*;

import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.domain.LatestSnapshot;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.external.ObjectStorageClient;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class LatestImageServiceTest {

    private static class InMemoryStorage implements ObjectStorageClient {
        final Map<String, byte[]> store = new HashMap<>();
        final com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();

        @Override
        public void putImage(String key, java.awt.image.BufferedImage image) { /* not used in these tests */ }

        @Override
        public void putJson(String key, Map<String, Object> payload) throws IOException {
            store.put(key, mapper.writeValueAsBytes(payload));
        }

        @Override
        public byte[] getObjectBytes(String key) throws IOException {
            byte[] b = store.get(key);
            if (b == null) throw new IOException("missing");
            return b;
        }

        @Override
        public boolean exists(String key) {
            return store.containsKey(key);
        }
    }

    @Test
    void resolveLatestTimestamp_readsImageId() throws Exception {
        InMemoryStorage storage = new InMemoryStorage();
        String key = "latest/latest.json";
        storage.putJson(key, Map.of("image_id", "2026/01/03/2010"));
        LatestImageService latest = new LatestImageService(storage, key, java.time.ZoneId.of("America/Los_Angeles"));

        Optional<java.time.ZonedDateTime> ts = latest.resolveLatestTimestamp();

        assertTrue(ts.isPresent());
        assertEquals(OffsetDateTime.parse("2026-01-04T04:10:00Z").toZonedDateTime(), ts.get());
    }

    @Test
    void resolveLatestTimestamp_derivesFromAnalysisKey() throws Exception {
        InMemoryStorage storage = new InMemoryStorage();
        String key = "latest/latest.json";
        storage.putJson(key, Map.of("analysis_s3_key", "analysis/v1/2026/01/03/2010.json"));
        LatestImageService latest = new LatestImageService(storage, key, java.time.ZoneId.of("America/Los_Angeles"));

        Optional<java.time.ZonedDateTime> ts = latest.resolveLatestTimestamp();

        assertTrue(ts.isPresent());
        assertEquals(OffsetDateTime.parse("2026-01-04T04:10:00Z").toZonedDateTime(), ts.get());
    }

    @Test
    void publishIfNew_writesWhenNewer_andSkipsWhenOlder() throws Exception {
        InMemoryStorage storage = new InMemoryStorage();
        String key = "latest/latest.json";
        LatestImageService latest = new LatestImageService(storage, key, java.time.ZoneId.of("America/Los_Angeles"));

        ImageContext ctxNew = new ImageContext();
        ctxNew.setImageId("2026/01/03/2010");
        ctxNew.setAnalysisS3Key("analysis/v1/2026/01/03/2010.json");
        ctxNew.setUpdatedAt("2026-01-03T20:20:00Z");

        latest.publishIfNew(ctxNew);
        LatestSnapshot snapshot = latest.readLatest().orElseThrow();
        assertEquals("2026/01/03/2010", snapshot.getImageId());

        ImageContext older = new ImageContext();
        older.setImageId("2026/01/03/2000");
        older.setUpdatedAt("2026-01-03T20:30:00Z");

        latest.publishIfNew(older);
        LatestSnapshot after = latest.readLatest().orElseThrow();
        assertEquals("2026/01/03/2010", after.getImageId(), "should not overwrite with older");
    }
}
