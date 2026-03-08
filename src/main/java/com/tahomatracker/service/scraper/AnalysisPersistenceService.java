package com.tahomatracker.service.scraper;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.tahomatracker.service.domain.DailyManifest;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.domain.ImageId;
import com.tahomatracker.service.domain.MonthlyManifest;
import com.tahomatracker.service.external.ObjectStorageClient;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import org.shredzone.commons.suncalc.SunTimes;

/**
 * Service for persisting analysis results and manifests to S3.
 * Supports multiple model versions - writes analysis and manifests for each version.
 * The last version in the list is considered the "primary" (latest) version.
 *
 * S3 layout:
 * - analysis/{version}/{imageId}.json
 * - manifests/daily/{version}/YYYY/MM/DD.json
 * - manifests/monthly/{version}/YYYY/MM.json
 * - manifests/daily/current.json (unversioned, uses primary version)
 * - manifests/monthly/current.json (unversioned, uses primary version)
 */
@Slf4j
public class AnalysisPersistenceService {

    private static final double CAMERA_LATITUDE = 47.6204;
    private static final double CAMERA_LONGITUDE = -122.3491;

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ObjectStorageClient storage;
    private final String analysisPrefix;
    private final String manifestsPrefix;
    private final String[] modelVersions;
    private final String primaryVersion;
    private final ZoneId localTz;

    public AnalysisPersistenceService(ObjectStorageClient storage, String analysisPrefix,
                                      String manifestsPrefix, String[] modelVersions, ZoneId localTz) {
        this.storage = storage;
        this.analysisPrefix = analysisPrefix;
        this.manifestsPrefix = manifestsPrefix;
        this.modelVersions = modelVersions;
        this.primaryVersion = modelVersions[modelVersions.length - 1];
        this.localTz = localTz;
        log.info("AnalysisPersistenceService initialized with versions: {}", String.join(", ", modelVersions));
    }

    /**
     * Persists analysis and updates manifests for all model versions.
     * Returns the analysis key for the primary version.
     */
    public String persistAnalysis(ImageContext context, ImageId imageId) throws IOException {
        String primaryKey = null;
        for (String version : modelVersions) {
            String key = persistAnalysisForVersion(context, imageId, version);
            if (version.equals(primaryVersion)) {
                primaryKey = key;
            }
        }
        return primaryKey;
    }

    private String persistAnalysisForVersion(ImageContext context, ImageId imageId, String version) throws IOException {
        String analysisKey = formatAnalysisKey(imageId, version);

        Map<String, Object> analysis = new HashMap<>();
        analysis.put("image_id", context.getImageId());
        analysis.put("frame_state_probabilities", context.getFrameStateProbabilities());
        analysis.put("visibility_probabilities", context.getVisibilityProbabilities());
        analysis.put("frame_state_model_version", version);
        analysis.put("visibility_model_version", version);
        analysis.put("cropped_s3_key", context.getCroppedS3Key());
        analysis.put("pano_s3_key", context.getPanoS3Key());
        analysis.put("updated_at", Instant.now().toString());

        storage.putJson(analysisKey, analysis);
        log.debug("Persisted analysis for {} version {}", imageId.getValue(), version);

        return analysisKey;
    }

    /**
     * Updates manifests for all model versions.
     */
    public void updateManifests(ImageContext context, ImageId imageId) {
        for (String version : modelVersions) {
            try {
                updateManifestsForVersion(context, imageId, version);
            } catch (Exception e) {
                log.error("Failed to update manifests for {} version {}: {}",
                        imageId.getValue(), version, e.getMessage(), e);
            }
        }
    }

    private void updateManifestsForVersion(ImageContext context, ImageId imageId, String version) throws IOException {
        // Update daily manifest
        DailyManifest daily = updateDailyManifest(imageId, context, version);

        // Update monthly manifest
        updateMonthlyManifest(imageId, daily.getSummary(), version);

        log.debug("Updated manifests for {} version {}", imageId.getValue(), version);
    }

    private DailyManifest updateDailyManifest(ImageId imageId, ImageContext context, String version) throws IOException {
        String dateStr = formatDateString(imageId);
        String archivedKey = formatDailyKey(imageId, version);

        DailyManifest manifest = loadDailyManifest(archivedKey)
                .orElseGet(() -> DailyManifest.builder().date(dateStr).build());

        if (version.equals(primaryVersion)) {
            ensureDaylight(manifest, LocalDate.of(
                    Integer.parseInt(imageId.getYear()),
                    Integer.parseInt(imageId.getMonth()),
                    Integer.parseInt(imageId.getDay())));
        }

        DailyManifest.DailyManifestEntry entry = DailyManifest.DailyManifestEntry.builder()
                .time(imageId.getTime())
                .frameState(context.getFrameState() != null ? context.getFrameState().getValue() : null)
                .frameStateProb(context.getFrameStateProb())
                .visibility(context.getVisibility() != null ? context.getVisibility().getValue() : null)
                .visibilityProb(context.getVisibilityProb())
                .build();

        manifest.addOrUpdateEntry(entry);
        manifest.recalculateSummary();
        String nowIso = Instant.now().toString();
        manifest.setGeneratedAt(nowIso);
        manifest.setLastCheckedAt(nowIso);

        writeManifest(archivedKey, manifest);

        // Write to unversioned current.json only for primary version
        if (version.equals(primaryVersion) && isToday(imageId)) {
            writeManifest(manifestsPrefix + "/daily/current.json", manifest);
        }

        return manifest;
    }

    private void updateMonthlyManifest(ImageId imageId, DailyManifest.DailyManifestSummary daySummary, String version) throws IOException {
        String monthStr = formatMonthString(imageId);
        String archivedKey = formatMonthlyKey(imageId, version);

        MonthlyManifest manifest = loadMonthlyManifest(archivedKey)
                .orElseGet(() -> MonthlyManifest.builder().month(monthStr).build());

        manifest.updateDay(imageId.getDay(), daySummary);
        manifest.recalculateStats();
        String nowIso = Instant.now().toString();
        manifest.setGeneratedAt(nowIso);
        manifest.setLastCheckedAt(nowIso);

        writeManifest(archivedKey, manifest);

        // Write to unversioned current.json only for primary version
        if (version.equals(primaryVersion) && isCurrentMonth(imageId)) {
            writeManifest(manifestsPrefix + "/monthly/current.json", manifest);
        }
    }

    /**
     * Mark current manifests as checked (primary version only).
     */
    public void markCurrentManifestsChecked() {
        LocalDate today = LocalDate.now(localTz);
        String nowIso = Instant.now().toString();

        try {
            touchCurrentDaily(today, nowIso);
        } catch (Exception e) {
            log.error("Failed to mark daily current.json as checked: {}", e.getMessage(), e);
        }

        try {
            touchCurrentMonthly(today, nowIso);
        } catch (Exception e) {
            log.error("Failed to mark monthly current.json as checked: {}", e.getMessage(), e);
        }
    }

    private void touchCurrentDaily(LocalDate today, String nowIso) throws IOException {
        String dateStr = today.toString();
        String currentKey = manifestsPrefix + "/daily/current.json";

        DailyManifest manifest = loadDailyManifest(currentKey)
                .filter(m -> dateStr.equals(m.getDate()))
                .orElseGet(() -> DailyManifest.builder().date(dateStr).build());

        ensureDaylight(manifest, today);
        if (manifest.getSummary() == null) {
            manifest.recalculateSummary();
        }
        manifest.setLastCheckedAt(nowIso);
        if (manifest.getGeneratedAt() == null) {
            manifest.setGeneratedAt(nowIso);
        }
        writeManifest(currentKey, manifest);
    }

    private void touchCurrentMonthly(LocalDate today, String nowIso) throws IOException {
        String monthStr = String.format("%04d-%02d", today.getYear(), today.getMonthValue());
        String currentKey = manifestsPrefix + "/monthly/current.json";

        MonthlyManifest manifest = loadMonthlyManifest(currentKey)
                .filter(m -> monthStr.equals(m.getMonth()))
                .orElseGet(() -> MonthlyManifest.builder().month(monthStr).build());

        if (manifest.getStats() == null) {
            manifest.recalculateStats();
        }
        manifest.setLastCheckedAt(nowIso);
        if (manifest.getGeneratedAt() == null) {
            manifest.setGeneratedAt(nowIso);
        }
        writeManifest(currentKey, manifest);
    }

    /**
     * Gets latest processed timestamp from primary version's current manifest.
     */
    public Optional<java.time.ZonedDateTime> getLatestProcessedTimestamp() {
        String currentKey = manifestsPrefix + "/daily/current.json";
        Optional<DailyManifest> manifestOpt = loadDailyManifest(currentKey);

        if (manifestOpt.isEmpty()) {
            return Optional.empty();
        }

        DailyManifest manifest = manifestOpt.get();
        var images = manifest.getImages();
        if (images == null || images.isEmpty()) {
            return Optional.empty();
        }

        var lastEntry = images.get(images.size() - 1);
        String dateStr = manifest.getDate();
        String time = lastEntry.getTime();

        try {
            String imageIdStr = dateStr.replace("-", "/") + "/" + time;
            ImageId imageId = ImageId.parse(imageIdStr);
            return Optional.of(imageId.toInstant(localTz).atZone(java.time.ZoneOffset.UTC));
        } catch (Exception e) {
            log.warn("Failed to parse latest timestamp from manifest: {}", e.getMessage());
            return Optional.empty();
        }
    }

    public String formatAnalysisKey(ImageId imageId) {
        return formatAnalysisKey(imageId, primaryVersion);
    }

    private String formatAnalysisKey(ImageId imageId, String version) {
        return analysisPrefix + "/" + version + "/" + imageId.getValue() + ".json";
    }

    private String formatDailyKey(ImageId imageId, String version) {
        return String.format("%s/daily/%s/%s/%s/%s.json",
                manifestsPrefix, version, imageId.getYear(), imageId.getMonth(), imageId.getDay());
    }

    private String formatMonthlyKey(ImageId imageId, String version) {
        return String.format("%s/monthly/%s/%s/%s.json",
                manifestsPrefix, version, imageId.getYear(), imageId.getMonth());
    }

    private String formatDateString(ImageId imageId) {
        return String.format("%s-%s-%s", imageId.getYear(), imageId.getMonth(), imageId.getDay());
    }

    private String formatMonthString(ImageId imageId) {
        return String.format("%s-%s", imageId.getYear(), imageId.getMonth());
    }

    private boolean isToday(ImageId imageId) {
        LocalDate imageDate = LocalDate.of(
                Integer.parseInt(imageId.getYear()),
                Integer.parseInt(imageId.getMonth()),
                Integer.parseInt(imageId.getDay()));
        return imageDate.equals(LocalDate.now(localTz));
    }

    private boolean isCurrentMonth(ImageId imageId) {
        int imageYear = Integer.parseInt(imageId.getYear());
        int imageMonth = Integer.parseInt(imageId.getMonth());
        LocalDate today = LocalDate.now(localTz);
        return imageYear == today.getYear() && imageMonth == today.getMonthValue();
    }

    private Optional<DailyManifest> loadDailyManifest(String key) {
        try {
            byte[] bytes = storage.getObjectBytes(key);
            return Optional.of(MAPPER.readValue(bytes, DailyManifest.class));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private Optional<MonthlyManifest> loadMonthlyManifest(String key) {
        try {
            byte[] bytes = storage.getObjectBytes(key);
            return Optional.of(MAPPER.readValue(bytes, MonthlyManifest.class));
        } catch (IOException e) {
            return Optional.empty();
        }
    }

    private void writeManifest(String key, Object manifest) throws IOException {
        Map<String, Object> map = MAPPER.convertValue(manifest,
                MAPPER.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class));
        storage.putJson(key, map);
    }

    private void ensureDaylight(DailyManifest manifest, LocalDate date) {
        if (manifest.getDaylight() != null
                && manifest.getDaylight().getSunriseAt() != null
                && manifest.getDaylight().getSunsetAt() != null) {
            return;
        }

        SunTimes sunTimes = SunTimes.compute()
                .on(date)
                .timezone(localTz)
                .at(CAMERA_LATITUDE, CAMERA_LONGITUDE)
                .execute();

        var sunrise = sunTimes.getRise();
        var sunset = sunTimes.getSet();

        if (sunrise == null || sunset == null || sunTimes.isAlwaysUp() || sunTimes.isAlwaysDown()) {
            throw new IllegalStateException("Unable to compute sunrise/sunset for " + date);
        }

        manifest.setDaylight(DailyManifest.DaylightInfo.builder()
                .sunriseAt(sunrise.toInstant().toString())
                .sunsetAt(sunset.toInstant().toString())
                .build());
    }
}
