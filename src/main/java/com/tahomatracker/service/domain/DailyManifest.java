package com.tahomatracker.service.domain;

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a manifest of all images processed for a single day.
 *
 * Used by the frontend to:
 * - Load all images for a day in a single request (no brute-force probing)
 * - Color-code the timeline scrubber by visibility state
 * - Know exactly which timestamps have images (handles gaps)
 *
 * S3 layout:
 * - manifests/daily/YYYY/MM/DD.json (archived, long TTL)
 * - manifests/daily/current.json (today only, short TTL)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DailyManifest {

    /**
     * Date in format "YYYY-MM-DD".
     */
    private String date;

    /**
     * List of all images for this day, sorted by time ascending.
     */
    @Builder.Default
    private List<DailyManifestEntry> images = new ArrayList<>();

    /**
     * Summary statistics for this day.
     */
    private DailyManifestSummary summary;

    /**
     * ISO timestamp when this manifest was last updated.
     */
    private String generatedAt;

    /**
     * ISO timestamp when we last checked for updates (even if nothing changed).
     */
    private String lastCheckedAt;

    /**
     * A single image entry in the daily manifest.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DailyManifestEntry {
        /**
         * Time portion of the imageId in format "HHmm" (e.g., "0400", "1430").
         */
        private String time;

        /**
         * Frame state: "good", "dark", "bad", "off_target".
         */
        private String frameState;

        /**
         * Visibility: "out", "partially_out", "not_out", or null if frameState != "good".
         */
        private String visibility;

        /**
         * Visibility probability (0.0-1.0), or null if not applicable.
         */
        private Double visibilityProb;
    }

    /**
     * Summary statistics for the day.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class DailyManifestSummary {
        /**
         * Total number of images for this day.
         */
        private int total;

        /**
         * Number of images with visibility="out".
         */
        private int outCount;

        /**
         * Number of images with visibility="partially_out".
         */
        private int partiallyOutCount;

        /**
         * Whether any image had visibility="out".
         */
        private boolean hadOut;

        /**
         * Whether any image had visibility="partially_out".
         */
        private boolean hadPartiallyOut;
    }

    /**
     * Adds or updates an entry for the given time.
     * If an entry for this time already exists, it is replaced.
     */
    public void addOrUpdateEntry(DailyManifestEntry entry) {
        // Remove existing entry for this time if present
        images.removeIf(e -> e.getTime().equals(entry.getTime()));
        images.add(entry);
        // Sort by time ascending
        images.sort((a, b) -> a.getTime().compareTo(b.getTime()));
    }

    /**
     * Recalculates the summary based on current entries.
     */
    public void recalculateSummary() {
        int total = images.size();
        int outCount = 0;
        int partiallyOutCount = 0;

        for (DailyManifestEntry entry : images) {
            if ("out".equals(entry.getVisibility())) {
                outCount++;
            } else if ("partially_out".equals(entry.getVisibility())) {
                partiallyOutCount++;
            }
        }

        this.summary = DailyManifestSummary.builder()
                .total(total)
                .outCount(outCount)
                .partiallyOutCount(partiallyOutCount)
                .hadOut(outCount > 0)
                .hadPartiallyOut(partiallyOutCount > 0)
                .build();
    }
}
