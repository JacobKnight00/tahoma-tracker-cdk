package com.tahomatracker.service.scraper;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.tahomatracker.service.domain.DailyManifest;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.domain.ImageId;
import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;
import com.tahomatracker.service.external.ObjectStorageClient;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AnalysisPersistenceServiceTest {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final ZoneId LOCAL_TZ = ZoneId.of("America/Los_Angeles");

    @Test
    void updateManifests_addsDaylightOnlyToPrimaryVersionDailyManifest() throws IOException {
        InMemoryStorageClient storage = new InMemoryStorageClient();
        AnalysisPersistenceService service = new AnalysisPersistenceService(
                storage, "analysis", "manifests", new String[]{"v1", "v2"}, LOCAL_TZ);

        ImageContext context = new ImageContext();
        context.setImageId("2025/08/23/1840");
        context.setFrameState(FrameState.GOOD);
        context.setFrameStateProb(0.97);
        context.setVisibility(Visibility.OUT);
        context.setVisibilityProb(0.91);

        service.updateManifests(context, ImageId.parse("2025/08/23/1840"));

        DailyManifest primaryManifest = storage.readJson(
                "manifests/daily/v2/2025/08/23.json", DailyManifest.class);
        assertNotNull(primaryManifest.getDaylight());
        assertNotNull(primaryManifest.getDaylight().getSunriseAt());
        assertNotNull(primaryManifest.getDaylight().getSunsetAt());
        assertTrue(primaryManifest.getDaylight().getSunriseAt().endsWith("Z"));
        assertTrue(primaryManifest.getDaylight().getSunsetAt().endsWith("Z"));
        assertTrue(Instant.parse(primaryManifest.getDaylight().getSunriseAt())
                .isBefore(Instant.parse(primaryManifest.getDaylight().getSunsetAt())));

        DailyManifest secondaryManifest = storage.readJson(
                "manifests/daily/v1/2025/08/23.json", DailyManifest.class);
        assertNull(secondaryManifest.getDaylight());
    }

    @Test
    void markCurrentManifestsChecked_initializesDaylightOnCurrentManifest() throws IOException {
        InMemoryStorageClient storage = new InMemoryStorageClient();
        AnalysisPersistenceService service = new AnalysisPersistenceService(
                storage, "analysis", "manifests", new String[]{"v2"}, LOCAL_TZ);

        service.markCurrentManifestsChecked();

        DailyManifest currentManifest = storage.readJson("manifests/daily/current.json", DailyManifest.class);
        assertEquals(LocalDate.now(LOCAL_TZ).toString(), currentManifest.getDate());
        assertNotNull(currentManifest.getDaylight());
        assertNotNull(currentManifest.getDaylight().getSunriseAt());
        assertNotNull(currentManifest.getDaylight().getSunsetAt());
    }

    private static class InMemoryStorageClient implements ObjectStorageClient {
        private final Map<String, byte[]> objects = new HashMap<>();

        @Override
        public void putImage(String key, BufferedImage image) {
            throw new UnsupportedOperationException("Not needed for test");
        }

        @Override
        public void putJson(String key, Map<String, Object> payload) throws IOException {
            objects.put(key, MAPPER.writeValueAsBytes(payload));
        }

        @Override
        public byte[] getObjectBytes(String key) throws IOException {
            byte[] value = objects.get(key);
            if (value == null) {
                throw new IOException("Missing object: " + key);
            }
            return value;
        }

        @Override
        public boolean exists(String key) {
            return objects.containsKey(key);
        }

        <T> T readJson(String key, Class<T> type) throws IOException {
            return MAPPER.readValue(getObjectBytes(key), type);
        }
    }
}
