package com.tahomatracker.backfill;

import com.tahomatracker.service.ScraperConfig;
import com.tahomatracker.service.domain.CropBox;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.classifier.OnnxFrameStateClassifier;
import com.tahomatracker.service.classifier.OnnxModelLoader;
import com.tahomatracker.service.classifier.OnnxVisibilityClassifier;
import com.tahomatracker.service.external.FastSliceFetcher;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.S3ObjectStorageClient;
import com.tahomatracker.service.external.SliceFetcher;
import com.tahomatracker.service.scraper.AnalysisPersistenceService;
import com.tahomatracker.service.scraper.ImageAcquisitionService;
import com.tahomatracker.service.scraper.ImageClassificationService;
import com.tahomatracker.service.scraper.ImageScrapingService;
import com.tahomatracker.service.scraper.TimeWindowPlanner;
import software.amazon.awssdk.services.s3.S3Client;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Backfill runner for scraping historical images.
 * Fetches panos, crops, classifies, persists analysis, and updates manifests.
 *
 * For generating classifications for a new model version on existing images,
 * use the Python backfill_model.py script in training/.
 */
public class ImageBackfillRunner {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ImageBackfillRunner.class);
    private final String bucketName;
    private final S3Client s3Client;
    private final FastSliceFetcher fetcher;
    private final String panosPrefix;
    private final String croppedPrefix;
    private final String analysisPrefix;
    private final String manifestsPrefix;
    private final String cropBox;
    private final ZoneId localTz;

    public ImageBackfillRunner(String cameraBaseUrl, String bucketName, S3Client s3Client,
                               String panosPrefix, String croppedPrefix, String analysisPrefix, String manifestsPrefix,
                               String cropBox, int concurrency, int batchSize) {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
        this.fetcher = new FastSliceFetcher(cameraBaseUrl, Math.max(2, concurrency), batchSize);
        this.panosPrefix = panosPrefix;
        this.croppedPrefix = croppedPrefix;
        this.analysisPrefix = analysisPrefix;
        this.manifestsPrefix = manifestsPrefix;
        this.cropBox = cropBox;
        this.localTz = ZoneId.of("America/Los_Angeles");
    }

    public void runRange(ZonedDateTime start, ZonedDateTime end, int stepMinutes,
                         boolean dryRun, int workers, String cameraBaseUrl) {
        int windowStart = 4;
        int windowEnd = 23;
        String modelsPrefix = "models";
        String modelVersion = "v1";
        String frameStateModelKey = modelsPrefix + "/" + modelVersion + "/frame_state_" + modelVersion + ".onnx";
        String visibilityModelKey = modelsPrefix + "/" + modelVersion + "/visibility_" + modelVersion + ".onnx";
        float[] mean = new float[]{0.485f, 0.456f, 0.406f};
        float[] std = new float[]{0.229f, 0.224f, 0.225f};

        ScraperConfig config = new ScraperConfig(
                bucketName,
                "",  // imageLabelsTableName not used in backfill
                cameraBaseUrl,
                panosPrefix,
                croppedPrefix,
                analysisPrefix,
                manifestsPrefix,
                CropBox.fromString(cropBox),
                0.85,
                modelVersion,
                localTz,
                windowStart,
                windowEnd,
                stepMinutes,
                24 * 365,
                modelsPrefix,
                frameStateModelKey,
                visibilityModelKey,
                224,
                224,
                mean,
                std
        );

        ObjectStorageClient s3Store = new S3ObjectStorageClient(s3Client, bucketName);
        SliceFetcher sliceFetcher = fetcher;
        var modelLoader = new OnnxModelLoader(s3Client, bucketName);
        var frameStateClassifier = new OnnxFrameStateClassifier(
                modelLoader,
                config.frameStateModelKey,
                config.modelInputWidth,
                config.modelInputHeight,
                config.normalizationMean,
                config.normalizationStd
        );
        var visibilityClassifier = new OnnxVisibilityClassifier(
                modelLoader,
                config.visibilityModelKey,
                config.modelInputWidth,
                config.modelInputHeight,
                config.normalizationMean,
                config.normalizationStd
        );

        ImageAcquisitionService imageAcquisition = new ImageAcquisitionService(
                sliceFetcher, s3Store, panosPrefix, croppedPrefix);
        ImageClassificationService classification = new ImageClassificationService(
                frameStateClassifier, visibilityClassifier, s3Store);
        AnalysisPersistenceService persistence = new AnalysisPersistenceService(
                s3Store, analysisPrefix, manifestsPrefix, new String[]{modelVersion}, localTz);
        var timeWindow = new TimeWindowPlanner(config);
        ImageScrapingService scrapingService = new ImageScrapingService(config, timeWindow, imageAcquisition,
                classification, persistence, s3Store);

        if (dryRun) {
            ZonedDateTime ts = start;
            while (!ts.isAfter(end)) {
                ZonedDateTime local = ts.withZoneSameInstant(localTz);
                if (!(local.getHour() >= windowStart && local.getHour() < windowEnd)) {
                    log.debug("Skipping outside window: {} (local {})", ts, local);
                    ts = ts.plusMinutes(stepMinutes);
                    continue;
                }
                log.info("DRY RUN: {}", ts.toInstant());
                ts = ts.plusMinutes(stepMinutes);
            }
            return;
        }

        int workerCount = Math.max(1, workers);
        ExecutorService pool = Executors.newFixedThreadPool(workerCount);
        CompletionService<Void> completion = new ExecutorCompletionService<>(pool);
        int submitted = 0;

        try {
            ZonedDateTime ts = start;
            while (!ts.isAfter(end)) {
                ZonedDateTime current = ts;
                completion.submit(() -> {
                    processTimestamp(scrapingService, current);
                    return null;
                });
                submitted++;
                ts = ts.plusMinutes(stepMinutes);
            }

            for (int i = 0; i < submitted; i++) {
                try {
                    completion.take().get();
                } catch (ExecutionException ex) {
                    log.error("Worker failed", ex.getCause());
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdown();
        }
    }

    private void processTimestamp(ImageScrapingService scrapingService, ZonedDateTime tsUtc) {
        String tsIso = tsUtc.toInstant().toString();
        try {
            ImageContext ctx = scrapingService.processSingle(tsUtc);
            if (ctx == null) {
                log.info("Skipped {} (already processed or outside window)", tsIso);
                return;
            }
            if (ctx.getStatus() == AcquisitionStatus.OK) {
                log.info("Completed {} (status OK)", tsIso);
            } else {
                log.warn("Completed {} with non-OK status: {}", tsIso, ctx.getStatus());
            }
        } catch (Exception e) {
            log.error("Error processing {}: {}", tsIso, e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            log.error("Usage: ImageBackfillRunner --start <YYYY-MM-DD> --end <YYYY-MM-DD> --bucket <bucket> " +
                    "[--dry-run] [--concurrency <n>] [--batch-size <n>] [--workers <n>]");
            System.exit(2);
        }
        String start = null, end = null, bucket = null;
        boolean dryRun = false;
        int concurrency = 8;
        int batchSize = 32;
        int workers = 1;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--start": start = args[++i]; break;
                case "--end": end = args[++i]; break;
                case "--bucket": bucket = args[++i]; break;
                case "--dry-run": dryRun = true; break;
                case "--concurrency": concurrency = Integer.parseInt(args[++i]); break;
                case "--batch-size": batchSize = Integer.parseInt(args[++i]); break;
                case "--workers": workers = Integer.parseInt(args[++i]); break;
                default:
                    log.error("Unknown arg: {}", args[i]);
                    System.exit(2);
            }
        }
        if (start == null || end == null || bucket == null) {
            log.error("--start, --end and --bucket are required");
            System.exit(2);
        }

        ZonedDateTime s = java.time.LocalDate.parse(start).atStartOfDay(java.time.ZoneOffset.UTC);
        ZonedDateTime e = java.time.LocalDate.parse(end).atTime(23, 59, 59).atZone(java.time.ZoneOffset.UTC);

        String cameraBaseUrl = "https://d3omclagh7m7mg.cloudfront.net/assets";
        S3Client s3 = S3Client.create();
        ImageBackfillRunner runner = new ImageBackfillRunner(cameraBaseUrl, bucket, s3,
                "needle-cam/panos", "needle-cam/cropped-images", "analysis", "manifests",
                "3975,200,4575,650", concurrency, batchSize);

        runner.runRange(s, e, 10, dryRun, workers, cameraBaseUrl);
    }
}
