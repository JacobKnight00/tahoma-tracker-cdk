package com.tahomatracker.service.scraper;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.tahomatracker.service.ScraperConfig;
import com.tahomatracker.service.domain.AcquisitionResult;
import com.tahomatracker.service.domain.BackfillState;
import com.tahomatracker.service.domain.ClassificationResult;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.domain.ImageId;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.RoundshotFetcher;
import com.amazonaws.services.lambda.runtime.Context;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

/**
 * Orchestrates webcam image scraping: fetch, stitch, classify, and persist.
 *
 * Handles both scheduled runs (with automatic backfill) and single-image processing.
 */
@Slf4j
public class ImageScrapingService {

    // --- Temporary: Historical Roundshot backfill (remove once complete) ---
    private static final String BACKFILL_STATE_KEY = "backfill/roundshot-backfill-state.json";
    private static final String BACKFILL_STOP_AT = "2025/01/01/0000";
    private static final int BACKFILL_TIMESTAMPS_PER_RUN = 10;

    private static final ObjectMapper BACKFILL_MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final ScraperConfig config;
    private final TimeWindowPlanner timeWindow;
    private final ImageAcquisitionService imageAcquisition;
    private final ImageClassificationService classification;
    private final AnalysisPersistenceService persistence;
    private final ObjectStorageClient storage;

    public ImageScrapingService(ScraperConfig config,
                                TimeWindowPlanner timeWindow,
                                ImageAcquisitionService imageAcquisition,
                                ImageClassificationService classification,
                                AnalysisPersistenceService persistence,
                                ObjectStorageClient storage) {
        this.config = config;
        this.timeWindow = timeWindow;
        this.imageAcquisition = imageAcquisition;
        this.classification = classification;
        this.persistence = persistence;
        this.storage = storage;
    }

    public Map<String, Object> run(Context lambdaContext) throws IOException, InterruptedException {
        ZonedDateTime lastSuccessful = persistence.getLatestProcessedTimestamp()
                .orElse(ZonedDateTime.now(ZoneOffset.UTC).minusHours(24));
        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        log.info("Starting pipeline: lastSuccessful={}, now={}, lookbackHours={}",
                lastSuccessful, now, config.backfillLookbackHours);

        List<ZonedDateTime> candidates = timeWindow.generateCandidates(
                lastSuccessful, now, config.backfillLookbackHours);
        if (candidates.isEmpty()) {
            log.warn("No candidate timestamps to process (lastSuccessful={}, now={}, window={}:00-{}:00, step={}min)",
                    lastSuccessful, now, config.windowStartHour, config.windowEndHour, config.stepMinutes);
        } else {
            String candidateList = candidates.stream()
                    .map(ts -> timeWindow.imageId(timeWindow.bucketStartLocal(ts)).getValue())
                    .limit(10)
                    .reduce((a, b) -> a + ", " + b)
                    .orElse("");
            String suffix = candidates.size() > 10 ? ", ..." : "";
            log.info("Candidates to check (n={}): [{}{}]", candidates.size(), candidateList, suffix);
        }

        ImageContext mostRecent = null;
        int processed = 0;
        int skipped = 0;
        int failed = 0;

        for (ZonedDateTime ts : candidates) {
            if (lambdaContext != null && lambdaContext.getRemainingTimeInMillis() < 30000) {
                log.warn("Stopping backfill early: Lambda timeout approaching ({}ms remaining)",
                        lambdaContext.getRemainingTimeInMillis());
                break;
            }

            ZonedDateTime local = timeWindow.bucketStartLocal(ts);
            var imageId = timeWindow.imageId(local);
            String imageIdValue = imageId.getValue();

            try (var ignoredKey = MDC.putCloseable("image_id", imageIdValue)) {
                if (analysisExists(imageId)) {
                    log.debug("Skip {}: analysis already exists", imageIdValue);
                    skipped++;
                    continue;
                }

                log.info("Processing {}", imageIdValue);
                try {
                    ImageContext result = processBucket(local, imageId);
                    if (result != null && result.getStatus() == AcquisitionStatus.OK) {
                        mostRecent = result;
                        processed++;
                        log.info("Success {}: visibility={}, frameState={}",
                                imageIdValue, result.getVisibility(), result.getFrameState());
                    } else if (result != null) {
                        log.warn("Skip {}: {}", imageIdValue, result.getStatus());
                    }
                } catch (Exception ex) {
                    log.error("Failed {}: {}", imageIdValue, ex.getMessage(), ex);
                    failed++;
                }
            }
        }

        Map<String, Object> summary = new HashMap<>();
        summary.put("checked", candidates.size());
        summary.put("processed", processed);
        summary.put("skipped", skipped);
        summary.put("failed", failed);
        summary.put("last_successful", mostRecent != null ? mostRecent.getImageId() : null);

        persistence.markCurrentManifestsChecked();

        if (processed > 0) {
            log.info("Pipeline complete: processed={}, skipped={}, failed={}, latest={}",
                    processed, skipped, failed, mostRecent != null ? mostRecent.getImageId() : "none");
        } else {
            log.info("Pipeline complete: no new images processed (checked={}, skipped={}, failed={})",
                    candidates.size(), skipped, failed);
        }

        // If every non-skipped candidate failed, something is systematically broken
        int attempted = candidates.size() - skipped;
        if (attempted > 0 && failed == attempted) {
            throw new IOException("All " + attempted + " candidate timestamps failed processing");
        }

        // Temporary: historical Roundshot backfill (remove once complete)
        runHistoricalBackfill(lambdaContext);

        return summary;
    }

    private ImageContext processBucket(ZonedDateTime tsLocal, ImageId imageId) throws IOException, InterruptedException {
        String keyBase = imageId.getValue();
        String folder = timeWindow.folderPath(tsLocal);
        ImageContext context = buildContext(tsLocal, imageId);

        // Step 1: Acquire images (fetch, stitch, crop, upload)
        var panoResult = imageAcquisition.fetchAndStitchPano(folder);
        if (panoResult.getStatus() == AcquisitionStatus.IMAGES_NOT_FOUND) {
            context.setStatus(AcquisitionStatus.IMAGES_NOT_FOUND);
            return context;
        }

        BufferedImage cropped = imageAcquisition.createCrop(panoResult.getPano(), config.cropBox);
        AcquisitionResult acqResult = imageAcquisition.uploadImages(panoResult.getPano(), cropped, keyBase);

        context.setPanoS3Key(acqResult.getPanoS3Key());
        context.setCroppedS3Key(acqResult.getCroppedS3Key());
        context.setStatus(AcquisitionStatus.OK);

        // Step 2: Classify
        ClassificationResult classResult = classification.classify(cropped);
        context.setFrameState(classResult.getFrameState());
        context.setFrameStateProb(classResult.getFrameStateProb());
        context.setVisibility(classResult.getVisibility());
        context.setVisibilityProb(classResult.getVisibilityProb());
        context.setFrameStateProbabilities(toStringMap(classResult.getFrameStateProbabilities()));
        context.setVisibilityProbabilities(toStringMap(classResult.getVisibilityProbabilities()));

        // Step 3: Persist analysis JSON
        String analysisKey = persistence.persistAnalysis(context, imageId);
        context.setAnalysisS3Key(analysisKey);

        // Step 4: Update daily and monthly manifests
        persistence.updateManifests(context, imageId);

        return context;
    }

    /**
     * Processes a single timestamp (UTC) without running the full backfill loop.
     *
     * @param tsUtc timestamp in UTC
     * @return ImageContext for the run, or null if skipped/outside window
     */
    public ImageContext processSingle(ZonedDateTime tsUtc) throws IOException, InterruptedException {
        ZonedDateTime local = timeWindow.bucketStartLocal(tsUtc);
        var imageId = timeWindow.imageId(local);
        String imageIdValue = imageId.getValue();

        try (var ignoredKey = MDC.putCloseable("image_id", imageIdValue)) {
            if (analysisExists(imageId)) {
                log.info("Skipping {} (analysis exists) in processSingle", tsUtc);
                return null;
            }

            ImageContext result = processBucket(local, imageId);
            return result;
        }
    }

    // -----------------------------------------------------------------------
    // Temporary: Historical Roundshot backfill (remove once complete)
    // -----------------------------------------------------------------------

    /**
     * Processes N timestamps backwards from the backfill cursor.
     * For timestamps WITH existing analysis: fetches roundshot image, uploads (overwrites old
     * space needle image), keeps existing analysis unchanged.
     * For timestamps WITHOUT existing analysis: full processing (fetch, crop, classify, persist).
     *
     * Stops on 429/5xx (retried next Lambda run). Skips 404/403 (no image available).
     */
    private void runHistoricalBackfill(Context lambdaContext) {
        try {
            BackfillState state = loadOrCreateBackfillState();
            if (!"in_progress".equals(state.getStatus())) {
                return;
            }

            ZonedDateTime cursor = ImageId.parse(state.getCursor()).toZonedDateTime(config.localTz);
            ZonedDateTime stop = ImageId.parse(state.getStopAt()).toZonedDateTime(config.localTz);

            String lastHandledCursor = state.getCursor();
            int processed = 0;

            for (int i = 0; i < BACKFILL_TIMESTAMPS_PER_RUN; i++) {
                if (lambdaContext != null && lambdaContext.getRemainingTimeInMillis() < 30000) {
                    log.warn("Historical backfill: stopping early (Lambda timeout)");
                    break;
                }

                cursor = cursor.minusMinutes(config.stepMinutes);

                if (!cursor.isAfter(stop)) {
                    log.info("Historical backfill: reached stop_at {}", state.getStopAt());
                    state.setStatus("completed");
                    break;
                }

                ImageId imageId = ImageId.fromZonedDateTime(cursor);

                try (var ignored = MDC.putCloseable("image_id", imageId.getValue())) {
                    try {
                        processBackfillTimestamp(cursor, imageId);
                        lastHandledCursor = imageId.getValue();
                        processed++;
                    } catch (RoundshotFetcher.RetryableException e) {
                        log.warn("Historical backfill: retryable error on {} (HTTP {}), stopping",
                                imageId.getValue(), e.getStatusCode());
                        break;
                    } catch (Exception e) {
                        log.error("Historical backfill: error on {}: {}, stopping",
                                imageId.getValue(), e.getMessage(), e);
                        break;
                    }
                }
            }

            state.setCursor(lastHandledCursor);
            state.setLastUpdated(Instant.now().toString());
            saveBackfillState(state);

            log.info("Historical backfill: processed={}, cursor={}, status={}",
                    processed, state.getCursor(), state.getStatus());
        } catch (Exception e) {
            log.error("Historical backfill: unexpected error: {}", e.getMessage(), e);
        }
    }

    private void processBackfillTimestamp(ZonedDateTime local, ImageId imageId)
            throws IOException, InterruptedException {
        String folder = timeWindow.folderPath(local);
        String keyBase = imageId.getValue();
        boolean hasExistingAnalysis = analysisExists(imageId);

        // Fetch roundshot image (throws RetryableException on 429/5xx)
        var panoResult = imageAcquisition.fetchAndStitchPano(folder);
        if (panoResult.getStatus() == AcquisitionStatus.IMAGES_NOT_FOUND) {
            log.debug("Historical backfill: no image for {}", imageId.getValue());
            return;
        }

        BufferedImage cropped = imageAcquisition.createCrop(panoResult.getPano(), config.cropBox);
        imageAcquisition.uploadImages(panoResult.getPano(), cropped, keyBase);

        if (hasExistingAnalysis) {
            log.info("Historical backfill: image replaced for {} (analysis kept)", imageId.getValue());
        } else {
            // No prior analysis — full processing
            ImageContext context = buildContext(local, imageId);
            context.setPanoS3Key(config.panosPrefix + "/" + keyBase + ".jpg");
            context.setCroppedS3Key(config.croppedPrefix + "/" + keyBase + ".jpg");
            context.setStatus(AcquisitionStatus.OK);

            ClassificationResult classResult = classification.classify(cropped);
            context.setFrameState(classResult.getFrameState());
            context.setFrameStateProb(classResult.getFrameStateProb());
            context.setVisibility(classResult.getVisibility());
            context.setVisibilityProb(classResult.getVisibilityProb());
            context.setFrameStateProbabilities(toStringMap(classResult.getFrameStateProbabilities()));
            context.setVisibilityProbabilities(toStringMap(classResult.getVisibilityProbabilities()));

            persistence.persistAnalysis(context, imageId);
            persistence.updateManifests(context, imageId);

            log.info("Historical backfill: full processing for {} (frameState={}, visibility={})",
                    imageId.getValue(), context.getFrameState(), context.getVisibility());
        }
    }

    private BackfillState loadOrCreateBackfillState() throws IOException {
        try {
            byte[] bytes = storage.getObjectBytes(BACKFILL_STATE_KEY);
            return BACKFILL_MAPPER.readValue(bytes, BackfillState.class);
        } catch (IOException e) {
            // First run — initialize state with cursor at current bucket time
            ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
            ZonedDateTime local = timeWindow.bucketStartLocal(now);
            String cursor = ImageId.fromZonedDateTime(local).getValue();

            BackfillState state = BackfillState.builder()
                    .cursor(cursor)
                    .stopAt(BACKFILL_STOP_AT)
                    .status("in_progress")
                    .lastUpdated(Instant.now().toString())
                    .build();

            saveBackfillState(state);
            log.info("Historical backfill: initialized state (cursor={}, stopAt={})", cursor, BACKFILL_STOP_AT);
            return state;
        }
    }

    @SuppressWarnings("unchecked")
    private void saveBackfillState(BackfillState state) throws IOException {
        Map<String, Object> map = BACKFILL_MAPPER.convertValue(state, Map.class);
        storage.putJson(BACKFILL_STATE_KEY, map);
    }

    // -----------------------------------------------------------------------

    private boolean analysisExists(ImageId imageId) {
        String key = persistence.formatAnalysisKey(imageId);
        try {
            return storage.exists(key);
        } catch (IOException ex) {
            return false;
        }
    }

    private ImageContext buildContext(ZonedDateTime local, ImageId imageId) {
        ImageContext context = new ImageContext();
        context.setImageId(imageId.getValue());
        context.setCropBox(config.cropBox);
        context.setFrameStateModelVersion(config.modelVersion);
        context.setVisibilityModelVersion(config.modelVersion);
        return context;
    }

    private Map<String, Double> toStringMap(Map<?, Double> probs) {
        if (probs == null) {
            return null;
        }
        Map<String, Double> out = new HashMap<>();
        probs.forEach((k, v) -> {
            if (k != null && v != null) {
                if (k instanceof com.tahomatracker.service.enums.FrameState) {
                    out.put(((com.tahomatracker.service.enums.FrameState) k).getValue(), v);
                } else if (k instanceof com.tahomatracker.service.enums.Visibility) {
                    out.put(((com.tahomatracker.service.enums.Visibility) k).getValue(), v);
                } else {
                    out.put(k.toString(), v);
                }
            }
        });
        return out;
    }
}
