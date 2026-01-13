package com.tahomatracker.service.scraper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.domain.LatestSnapshot;
import com.tahomatracker.service.external.ObjectStorageClient;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.Map;

/**
 * Handles reading/writing latest.json and timestamp resolution.
 */
@Slf4j
public class LatestImageService {
    private final ObjectStorageClient storage;
    private final String latestKey;
    private final ZoneId localTz;
    private final ObjectMapper mapper = new ObjectMapper();

    public LatestImageService(ObjectStorageClient storage, String latestKey, ZoneId localTz) {
        this.storage = storage;
        this.latestKey = latestKey;
        this.localTz = localTz;
    }

    public Optional<LatestSnapshot> readLatest() {
        try {
            byte[] bytes = storage.getObjectBytes(latestKey);
            Map<?, ?> raw = mapper.readValue(bytes, Map.class);
            return Optional.of(mapper.convertValue(raw, LatestSnapshot.class));
        } catch (IOException ex) {
            log.debug("Could not read latest.json: {}", ex.getMessage());
            return Optional.empty();
        }
    }

    public void publishIfNew(ImageContext context) throws IOException {
        ZonedDateTime currentLatest = resolveLatestTimestamp()
                .orElse(OffsetDateTime.parse("1970-01-01T00:00:00Z").toZonedDateTime());
        ZonedDateTime newTimestamp = parseImageIdAsZonedDateTime(context.getImageId());

        if (newTimestamp != null && newTimestamp.isAfter(currentLatest)) {
            LatestSnapshot snapshot = LatestSnapshot.builder()
                    .imageId(context.getImageId())
                    .frameStateProbabilities(context.getFrameStateProbabilities())
                    .visibilityProbabilities(context.getVisibilityProbabilities())
                    .frameStateModelVersion(context.getFrameStateModelVersion())
                    .visibilityModelVersion(context.getVisibilityModelVersion())
                    .croppedS3Key(context.getCroppedS3Key())
                    .analysisS3Key(context.getAnalysisS3Key())
                    .updatedAt(context.getUpdatedAt())
                    .build();

            @SuppressWarnings("unchecked")
            java.util.Map<String, Object> snapshotMap = mapper.convertValue(snapshot, java.util.Map.class);
            storage.putJson(latestKey, snapshotMap);
            log.info("Published latest.json for {}", context.getImageId());
        } else {
            log.info("Skipped latest.json update: {} is not newer than current {}", newTimestamp, currentLatest);
        }
    }

    public Optional<ZonedDateTime> resolveLatestTimestamp() {
        var latestOpt = readLatest();
        if (latestOpt.isPresent()) {
            LatestSnapshot latest = latestOpt.get();
            ZonedDateTime fromImageId = parseImageIdAsZonedDateTime(latest.getImageId());
            if (fromImageId != null) {
                return Optional.of(fromImageId);
            }
            // fallback: derive from analysis key
            ZonedDateTime fromKey = parseFromImageKey(latest.getAnalysisS3Key());
            if (fromKey != null) {
                log.info("Derived latest timestamp {} from analysis key {}", fromKey, latest.getAnalysisS3Key());
                return Optional.of(fromKey);
            }
        }
        return Optional.empty();
    }

    private ZonedDateTime parseImageIdAsZonedDateTime(String imageId) {
        try {
            if (imageId == null || imageId.isBlank()) {
                return null;
            }
            return com.tahomatracker.service.domain.ImageId.parse(imageId)
                    .toInstant(localTz)
                    .atZone(java.time.ZoneOffset.UTC);
        } catch (Exception ignored) {
            return null;
        }
    }

    private ZonedDateTime parseFromImageKey(String analysisKey) {
        if (analysisKey == null || analysisKey.isBlank()) {
            return null;
        }
        String[] parts = analysisKey.split("/");
        if (parts.length < 4) {
            return null;
        }
        // Expect .../yyyy/MM/dd/HHmm.json
        int n = parts.length;
        String imageId = String.join("/", parts[n - 4], parts[n - 3], parts[n - 2], parts[n - 1].replace(".json", ""));
        return parseImageIdAsZonedDateTime(imageId);
    }
}
