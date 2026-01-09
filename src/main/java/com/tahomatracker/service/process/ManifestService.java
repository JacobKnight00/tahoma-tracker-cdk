package com.tahomatracker.service.process;

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

/**
 * Service for managing daily and monthly manifests.
 *
 * Manifests are updated after each successful image processing to provide
 * the frontend with efficient access to all images for a day/month without
 * brute-force probing.
 *
 * S3 layout:
 * - manifests/daily/YYYY/MM/DD.json (archived, long TTL)
 * - manifests/daily/current.json (today only, short TTL)
 * - manifests/monthly/YYYY/MM.json (archived, long TTL)
 * - manifests/monthly/current.json (current month only, short TTL)
 *
 * Day/Month Rollover:
 * When a new day or month starts, the first image processed will:
 * 1. Create a fresh manifest for the new date (archived location doesn't exist yet)
 * 2. Overwrite current.json with this new manifest
 * This naturally handles the transition without special rollover logic.
 */
@Slf4j
public class ManifestService {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ObjectStorageClient storage;
    private final String manifestsPrefix;
    private final ZoneId localTz;

    public ManifestService(ObjectStorageClient storage, String manifestsPrefix, ZoneId localTz) {
        this.storage = storage;
        this.manifestsPrefix = manifestsPrefix;
        this.localTz = localTz;
    }

    /**
     * Updates both daily and monthly manifests after a successful image processing.
     *
     * @param context The processed image context with classification results
     * @param imageId The image identifier
     */
    public void updateManifests(ImageContext context, ImageId imageId) {
        try {
            // Update daily manifest
            DailyManifest daily = updateDailyManifest(imageId, context);

            // Update monthly manifest with the daily summary
            updateMonthlyManifest(imageId, daily.getSummary());

            log.info("Updated manifests for {}", imageId.getValue());
        } catch (Exception e) {
            // Log error but don't fail the pipeline - manifests are not critical
            log.error("Failed to update manifests for {}: {}", imageId.getValue(), e.getMessage(), e);
        }
    }

    /**
     * Updates the daily manifest for the given imageId's date with a new entry.
     */
    private DailyManifest updateDailyManifest(ImageId imageId, ImageContext context) throws IOException {
        String dateStr = formatDateString(imageId);
        String archivedKey = formatDailyKey(imageId);

        // Load existing manifest or create new one
        DailyManifest manifest = loadDailyManifest(archivedKey)
                .orElseGet(() -> DailyManifest.builder()
                        .date(dateStr)
                        .build());

        // Create entry for this image
        DailyManifest.DailyManifestEntry entry = DailyManifest.DailyManifestEntry.builder()
                .time(imageId.getTime())
                .frameState(context.getFrameState() != null ? context.getFrameState().getValue() : null)
                .visibility(context.getVisibility() != null ? context.getVisibility().getValue() : null)
                .visibilityProb(context.getVisibilityProb())
                .build();

        manifest.addOrUpdateEntry(entry);
        manifest.recalculateSummary();
        String nowIso = Instant.now().toString();
        manifest.setGeneratedAt(nowIso);
        manifest.setLastCheckedAt(nowIso);

        // Write to archived location
        writeManifest(archivedKey, manifest);

        // Also write to current.json if this is today
        // On day rollover, this overwrites yesterday's manifest with today's fresh one
        if (isToday(imageId)) {
            String currentKey = manifestsPrefix + "/daily/current.json";
            writeManifest(currentKey, manifest);
            log.debug("Updated daily current.json for {}", dateStr);
        }

        return manifest;
    }

    /**
     * Updates the monthly manifest with the given daily summary.
     */
    private void updateMonthlyManifest(ImageId imageId, DailyManifest.DailyManifestSummary daySummary)
            throws IOException {
        String monthStr = formatMonthString(imageId);
        String archivedKey = formatMonthlyKey(imageId);

        // Load existing manifest or create new one
        MonthlyManifest manifest = loadMonthlyManifest(archivedKey)
                .orElseGet(() -> MonthlyManifest.builder()
                        .month(monthStr)
                        .build());

        manifest.updateDay(imageId.getDay(), daySummary);
        manifest.recalculateStats();
        String nowIso = Instant.now().toString();
        manifest.setGeneratedAt(nowIso);
        manifest.setLastCheckedAt(nowIso);

        // Write to archived location
        writeManifest(archivedKey, manifest);

        // Also write to current.json if this is the current month
        // On month rollover, this overwrites last month's manifest with the new month's fresh one
        if (isCurrentMonth(imageId)) {
            String currentKey = manifestsPrefix + "/monthly/current.json";
            writeManifest(currentKey, manifest);
            log.debug("Updated monthly current.json for {}", monthStr);
        }
    }

    /**
     * Loads a daily manifest from S3.
     */
    private Optional<DailyManifest> loadDailyManifest(String key) {
        try {
            byte[] bytes = storage.getObjectBytes(key);
            return Optional.of(MAPPER.readValue(bytes, DailyManifest.class));
        } catch (IOException e) {
            // Not found or parse error - return empty
            log.debug("Could not load daily manifest from {}: {}", key, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Loads a monthly manifest from S3.
     */
    private Optional<MonthlyManifest> loadMonthlyManifest(String key) {
        try {
            byte[] bytes = storage.getObjectBytes(key);
            return Optional.of(MAPPER.readValue(bytes, MonthlyManifest.class));
        } catch (IOException e) {
            // Not found or parse error - return empty
            log.debug("Could not load monthly manifest from {}: {}", key, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * Writes a manifest to S3.
     */
    private void writeManifest(String key, Object manifest) throws IOException {
        Map<String, Object> map = MAPPER.convertValue(manifest,
                MAPPER.getTypeFactory().constructMapType(HashMap.class, String.class, Object.class));
        storage.putJson(key, map);
    }

    /**
     * Mark the current daily and monthly manifests as checked, even if no new images were added.
     * This keeps current.json fresh when the scraper runs but finds nothing new.
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
                .orElseGet(() -> DailyManifest.builder()
                        .date(dateStr)
                        .build());

        if (manifest.getSummary() == null) {
            manifest.recalculateSummary();
        }

        manifest.setLastCheckedAt(nowIso);
        if (manifest.getGeneratedAt() == null) {
            manifest.setGeneratedAt(nowIso);
        }

        writeManifest(currentKey, manifest);
        log.debug("Marked daily current.json checked for {}", dateStr);
    }

    private void touchCurrentMonthly(LocalDate today, String nowIso) throws IOException {
        String monthStr = formatMonthString(today);
        String currentKey = manifestsPrefix + "/monthly/current.json";

        MonthlyManifest manifest = loadMonthlyManifest(currentKey)
                .filter(m -> monthStr.equals(m.getMonth()))
                .orElseGet(() -> MonthlyManifest.builder()
                        .month(monthStr)
                        .build());

        if (manifest.getStats() == null) {
            manifest.recalculateStats();
        }

        manifest.setLastCheckedAt(nowIso);
        if (manifest.getGeneratedAt() == null) {
            manifest.setGeneratedAt(nowIso);
        }

        writeManifest(currentKey, manifest);
        log.debug("Marked monthly current.json checked for {}", monthStr);
    }

    /**
     * Formats the date string from an ImageId.
     * Example: ImageId "2025/01/15/1430" → "2025-01-15"
     */
    private String formatDateString(ImageId imageId) {
        return String.format("%s-%s-%s", imageId.getYear(), imageId.getMonth(), imageId.getDay());
    }

    private String formatMonthString(LocalDate date) {
        return String.format("%04d-%02d", date.getYear(), date.getMonthValue());
    }

    /**
     * Formats the month string from an ImageId.
     * Example: ImageId "2025/01/15/1430" → "2025-01"
     */
    private String formatMonthString(ImageId imageId) {
        return String.format("%s-%s", imageId.getYear(), imageId.getMonth());
    }

    /**
     * Formats the S3 key for a daily manifest.
     * Format: {manifestsPrefix}/daily/YYYY/MM/DD.json
     */
    private String formatDailyKey(ImageId imageId) {
        return String.format("%s/daily/%s/%s/%s.json",
                manifestsPrefix, imageId.getYear(), imageId.getMonth(), imageId.getDay());
    }

    /**
     * Formats the S3 key for a monthly manifest.
     * Format: {manifestsPrefix}/monthly/YYYY/MM.json
     */
    private String formatMonthlyKey(ImageId imageId) {
        return String.format("%s/monthly/%s/%s.json",
                manifestsPrefix, imageId.getYear(), imageId.getMonth());
    }

    /**
     * Checks if the given imageId's date is today in the local timezone.
     */
    private boolean isToday(ImageId imageId) {
        LocalDate imageDate = LocalDate.of(
                Integer.parseInt(imageId.getYear()),
                Integer.parseInt(imageId.getMonth()),
                Integer.parseInt(imageId.getDay()));
        LocalDate today = LocalDate.now(localTz);
        return imageDate.equals(today);
    }

    /**
     * Checks if the given imageId's month is the current month in the local timezone.
     */
    private boolean isCurrentMonth(ImageId imageId) {
        int imageYear = Integer.parseInt(imageId.getYear());
        int imageMonth = Integer.parseInt(imageId.getMonth());
        LocalDate today = LocalDate.now(localTz);
        return imageYear == today.getYear() && imageMonth == today.getMonthValue();
    }
}
