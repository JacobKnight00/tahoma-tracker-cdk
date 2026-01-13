package com.tahomatracker.service.scraper;

import com.tahomatracker.service.ScraperConfig;
import com.tahomatracker.service.domain.AcquisitionResult;
import com.tahomatracker.service.domain.ClassificationResult;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.domain.ImageId;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.external.ObjectStorageClient;
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

    private final ScraperConfig config;
    private final TimeWindowPlanner timeWindow;
    private final ImageAcquisitionService imageAcquisition;
    private final ImageClassificationService classification;
    private final AnalysisPersistenceService persistence;
    private final ManifestService manifestService;
    private final ObjectStorageClient storage;

    public ImageScrapingService(ScraperConfig config,
                                TimeWindowPlanner timeWindow,
                                ImageAcquisitionService imageAcquisition,
                                ImageClassificationService classification,
                                AnalysisPersistenceService persistence,
                                ManifestService manifestService,
                                ObjectStorageClient storage) {
        this.config = config;
        this.timeWindow = timeWindow;
        this.imageAcquisition = imageAcquisition;
        this.classification = classification;
        this.persistence = persistence;
        this.manifestService = manifestService;
        this.storage = storage;
    }

    public Map<String, Object> run(Context lambdaContext) throws IOException, InterruptedException {
        ZonedDateTime lastSuccessful = manifestService.getLatestProcessedTimestamp()
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

                if (!timeWindow.withinWindow(local)) {
                    log.debug("Skip {}: outside window (hour={})", imageIdValue, local.getHour());
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
                }
            }
        }

        Map<String, Object> summary = new HashMap<>();
        summary.put("checked", candidates.size());
        summary.put("processed", processed);
        summary.put("skipped", skipped);
        summary.put("last_successful", mostRecent != null ? mostRecent.getImageId() : null);

        manifestService.markCurrentManifestsChecked();

        if (processed > 0) {
            log.info("Pipeline complete: processed={}, skipped={}, latest={}",
                    processed, skipped, mostRecent != null ? mostRecent.getImageId() : "none");
        } else {
            log.info("Pipeline complete: no new images processed (checked={}, skipped={})",
                    candidates.size(), skipped);
        }
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
        manifestService.updateManifests(context, imageId);

        return context;
    }

    /**
     * Processes a single timestamp (UTC) without running the full backfill loop.
     *
     * @param tsUtc timestamp in UTC
     * @param publishLatest whether to publish latest.json after a successful run
     * @return ImageContext for the run, or null if skipped/outside window
     */
    public ImageContext processSingle(ZonedDateTime tsUtc, boolean publishLatest) throws IOException, InterruptedException {
        ZonedDateTime local = timeWindow.bucketStartLocal(tsUtc);

        if (!timeWindow.withinWindow(local)) {
            log.info("Skipping {} (outside window) in processSingle", tsUtc);
            return null;
        }

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

    private String isoformat(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC)
                .withNano(0)
                .toString()
                .replace("+00:00", "Z");
    }
}
