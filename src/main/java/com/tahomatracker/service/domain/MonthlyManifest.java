package com.tahomatracker.service.domain;

import java.util.HashMap;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a manifest summarizing visibility data for an entire month.
 *
 * Used by the frontend to:
 * - Color-code calendar date selector (which days had "out" or "partially_out")
 * - Show monthly statistics
 * - Enable quick navigation to days with visibility
 *
 * S3 layout:
 * - manifests/monthly/YYYY/MM.json (archived, long TTL)
 * - manifests/monthly/current.json (current month only, short TTL)
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MonthlyManifest {

    /**
     * Month in format "YYYY-MM".
     */
    private String month;

    /**
     * Map of day number (two-digit string like "01", "15", "31") to day summary.
     * Only days with images are included (sparse).
     */
    @Builder.Default
    private Map<String, MonthlyDaySummary> days = new HashMap<>();

    /**
     * Aggregate statistics for the entire month.
     */
    private MonthlyStats stats;

    /**
     * ISO timestamp when this manifest was last updated.
     */
    private String generatedAt;

    /**
     * ISO timestamp when we last checked for updates (even if nothing changed).
     */
    private String lastCheckedAt;

    /**
     * Summary for a single day within the month.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class MonthlyDaySummary {
        /**
         * Whether any image on this day had visibility="out".
         */
        private boolean hadOut;

        /**
         * Whether any image on this day had visibility="partially_out".
         */
        private boolean hadPartiallyOut;

        /**
         * Total number of images for this day.
         */
        private int imageCount;

        /**
         * Number of images with visibility="out".
         */
        private int outCount;

        /**
         * Number of images with visibility="partially_out".
         */
        private int partiallyOutCount;
    }

    /**
     * Aggregate statistics for the month.
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    public static class MonthlyStats {
        /**
         * Number of days with at least one "out" image.
         */
        private int daysWithOut;

        /**
         * Number of days with at least one "partially_out" image.
         */
        private int daysWithPartiallyOut;

        /**
         * Total number of "out" images in the month.
         */
        private int totalOutImages;

        /**
         * Total number of "partially_out" images in the month.
         */
        private int totalPartiallyOutImages;

        /**
         * Total number of images in the month.
         */
        private int totalImages;
    }

    /**
     * Updates the summary for a specific day.
     *
     * @param day Two-digit day string (e.g., "01", "15")
     * @param daySummary The summary from the daily manifest
     */
    public void updateDay(String day, DailyManifest.DailyManifestSummary daySummary) {
        MonthlyDaySummary monthDay = MonthlyDaySummary.builder()
                .hadOut(daySummary.isHadOut())
                .hadPartiallyOut(daySummary.isHadPartiallyOut())
                .imageCount(daySummary.getTotal())
                .outCount(daySummary.getOutCount())
                .partiallyOutCount(daySummary.getPartiallyOutCount())
                .build();
        days.put(day, monthDay);
    }

    /**
     * Recalculates aggregate stats based on current day summaries.
     */
    public void recalculateStats() {
        int daysWithOut = 0;
        int daysWithPartiallyOut = 0;
        int totalOutImages = 0;
        int totalPartiallyOutImages = 0;
        int totalImages = 0;

        for (MonthlyDaySummary day : days.values()) {
            if (day.isHadOut()) {
                daysWithOut++;
            }
            if (day.isHadPartiallyOut()) {
                daysWithPartiallyOut++;
            }
            totalOutImages += day.getOutCount();
            totalPartiallyOutImages += day.getPartiallyOutCount();
            totalImages += day.getImageCount();
        }

        this.stats = MonthlyStats.builder()
                .daysWithOut(daysWithOut)
                .daysWithPartiallyOut(daysWithPartiallyOut)
                .totalOutImages(totalOutImages)
                .totalPartiallyOutImages(totalPartiallyOutImages)
                .totalImages(totalImages)
                .build();
    }
}
