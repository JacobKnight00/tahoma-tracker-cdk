package com.tahomatracker.service.process;

import static org.junit.jupiter.api.Assertions.*;

import com.tahomatracker.service.ScraperConfig;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class TimeWindowPlannerTest {

    @Test
    void bucketStartLocal_roundsDownToStep() {
        ScraperConfig config = new ScraperConfig();
        TimeWindowPlanner planner = new TimeWindowPlanner(config);
        ZonedDateTime ts = ZonedDateTime.of(2025, 1, 1, 12, 7, 30, 0, ZoneOffset.UTC);

        ZonedDateTime bucket = planner.bucketStartLocal(ts);

        assertEquals(0, bucket.getSecond());
        assertEquals(0, bucket.getNano());
        assertEquals(0, bucket.getMinute() % config.stepMinutes);
    }

    @Test
    void generateCandidates_advancesByStepWithinLookback() {
        ScraperConfig config = new ScraperConfig();
        TimeWindowPlanner planner = new TimeWindowPlanner(config);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        ZonedDateTime start = end.minusHours(1);

        List<ZonedDateTime> candidates = planner.generateCandidates(start, end, 2);

        assertFalse(candidates.isEmpty());
        for (int i = 1; i < candidates.size(); i++) {
            long minutes = java.time.Duration.between(candidates.get(i - 1), candidates.get(i)).toMinutes();
            assertEquals(config.stepMinutes, minutes);
        }
    }

    @Test
    void generateCandidates_clampsStartToCutoff() {
        ScraperConfig config = new ScraperConfig();
        TimeWindowPlanner planner = new TimeWindowPlanner(config);
        ZonedDateTime end = ZonedDateTime.now(ZoneOffset.UTC);
        ZonedDateTime start = end.minusHours(24); // far before lookback

        List<ZonedDateTime> candidates = planner.generateCandidates(start, end, 6);

        assertFalse(candidates.isEmpty());
        ZonedDateTime cutoff = end.minusHours(6);
        assertTrue(candidates.get(0).isAfter(cutoff) || candidates.get(0).isEqual(cutoff));
    }

    @Test
    void generateCandidates_excludesLastSuccessfulTimestamp() {
        ScraperConfig config = new ScraperConfig();
        TimeWindowPlanner planner = new TimeWindowPlanner(config);

        // Example: lastSuccessful = 3:10, now = 3:41
        ZonedDateTime lastSuccessful = ZonedDateTime.of(2025, 1, 1, 3, 10, 0, 0, ZoneOffset.UTC);
        ZonedDateTime now = ZonedDateTime.of(2025, 1, 1, 3, 41, 0, 0, ZoneOffset.UTC);

        List<ZonedDateTime> candidates = planner.generateCandidates(lastSuccessful, now, 6);

        // Should generate [3:20, 3:30, 3:40], NOT including 3:10
        assertEquals(3, candidates.size(), "Should have 3 candidates");
        assertFalse(candidates.contains(lastSuccessful), "Should NOT include lastSuccessful timestamp");
        assertEquals(ZonedDateTime.of(2025, 1, 1, 3, 20, 0, 0, ZoneOffset.UTC), candidates.get(0));
        assertEquals(ZonedDateTime.of(2025, 1, 1, 3, 30, 0, 0, ZoneOffset.UTC), candidates.get(1));
        assertEquals(ZonedDateTime.of(2025, 1, 1, 3, 40, 0, 0, ZoneOffset.UTC), candidates.get(2));
    }
}
