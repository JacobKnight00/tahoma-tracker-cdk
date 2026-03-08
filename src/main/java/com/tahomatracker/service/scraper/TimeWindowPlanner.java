package com.tahomatracker.service.scraper;

import com.tahomatracker.service.ScraperConfig;
import com.tahomatracker.service.domain.ImageId;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Handles time bucketing, window checks, and key formatting for processing runs.
 */
public class TimeWindowPlanner {
    // Roundshot camera path format: YYYY-MM-DD/HH-mm-00
    // Seconds are always 00 since Roundshot publishes on exact 10-minute marks.
    private static final DateTimeFormatter FOLDER_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd/HH-mm-'00'");
    private static final DateTimeFormatter KEY_FORMAT =
            DateTimeFormatter.ofPattern("yyyy/MM/dd/HHmm");

    private final ScraperConfig config;

    public TimeWindowPlanner(ScraperConfig config) {
        this.config = config;
    }

    public ZonedDateTime bucketStartLocal(ZonedDateTime tsUtc) {
        ZonedDateTime local = tsUtc.withZoneSameInstant(config.localTz);
        int minute = (local.getMinute() / config.stepMinutes) * config.stepMinutes;
        return local.withMinute(minute).withSecond(0).withNano(0);
    }

    public boolean withinWindow(ZonedDateTime local) {
        return local.getHour() >= config.windowStartHour && local.getHour() < config.windowEndHour;
    }

    public List<ZonedDateTime> generateCandidates(ZonedDateTime start, ZonedDateTime end, int maxLookbackHours) {
        List<ZonedDateTime> candidates = new ArrayList<>();
        ZonedDateTime cutoff = end.minusHours(maxLookbackHours);

        ZonedDateTime effectiveStart = start.isBefore(cutoff) ? cutoff : start;

        // Start from the next interval after last successful
        ZonedDateTime current = effectiveStart.plusMinutes(config.stepMinutes);

        while (!current.isAfter(end) && current.isAfter(cutoff)) {
            candidates.add(current);
            current = current.plusMinutes(config.stepMinutes);
        }

        return candidates;
    }

    public String folderPath(ZonedDateTime tsLocal) {
        return FOLDER_FORMAT.format(tsLocal);
    }

    public String keyBase(ZonedDateTime tsLocal) {
        return KEY_FORMAT.format(tsLocal);
    }

    /**
     * Returns an ImageId for the given local bucket start.
     */
    public ImageId imageId(ZonedDateTime tsLocal) {
        return ImageId.fromZonedDateTime(tsLocal);
    }

    public String formatKey(String prefix, ZonedDateTime tsLocal, String ext) {
        return prefix + "/" + keyBase(tsLocal) + "." + ext;
    }

    /**
     * Parses a key base (yyyy/MM/dd/HHmm) using the configured local timezone and returns the UTC instant.
     */
    public ZonedDateTime keyBaseToUtc(String keyBase) {
        LocalDateTime local = LocalDateTime.parse(keyBase, KEY_FORMAT);
        return local.atZone(config.localTz).withZoneSameInstant(java.time.ZoneOffset.UTC);
    }

    public String isoTimestampForKeyBase(String keyBase) {
        return java.time.format.DateTimeFormatter.ISO_INSTANT.format(keyBaseToUtc(keyBase).toInstant());
    }
}
