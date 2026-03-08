package com.tahomatracker.backfill;

import com.tahomatracker.service.ScraperConfig;
import com.tahomatracker.service.domain.CropBox;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.classifier.OnnxFrameStateClassifier;
import com.tahomatracker.service.classifier.OnnxModelLoader;
import com.tahomatracker.service.classifier.OnnxVisibilityClassifier;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.ImageFetcher;
import com.tahomatracker.service.external.RoundshotFetcher;
import com.tahomatracker.service.external.S3ObjectStorageClient;
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
 * Backfill runner for scraping historical images from Roundshot.
 * Fetches panos, crops, classifies, persists analysis, and updates manifests.
 *
 * <p>For generating classifications for a new model version on existing images
 * (without re-downloading), use the Python backfill_model.py script in training/.
 *
 * <p>Usage:
 * <pre>
 *   ImageBackfillRunner \
 *     --start 2025-12-01 --end 2026-02-28 --bucket my-bucket \
 *     [--camera-base-url https://storage.roundshot.com/544a1a9d451563.40343637] \
 *     [--crop-box 7550,400,8750,1200] \
 *     [--model-version v3] \
 *     [--panos-prefix needle-cam/roundshot-panos] \
 *     [--cropped-prefix needle-cam/roundshot-cropped-images] \
 *     [--workers 4] \
 *     [--dry-run]
 * </pre>
 */
public class ImageBackfillRunner {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ImageBackfillRunner.class);

    private static final String DEFAULT_CAMERA_BASE_URL =
            "https://storage.roundshot.com/544a1a9d451563.40343637";
    private static final String DEFAULT_CROP_BOX      = "7550,400,8750,1200";
    private static final String DEFAULT_MODEL_VERSION = "v3";
    private static final String DEFAULT_PANOS_PREFIX  = "needle-cam/roundshot-panos";
    private static final String DEFAULT_CROPPED_PREFIX = "needle-cam/roundshot-cropped-images";

    private final String cameraBaseUrl;
    private final String bucketName;
    private final S3Client s3Client;
    private final String panosPrefix;
    private final String croppedPrefix;
    private final String analysisPrefix;
    private final String manifestsPrefix;
    private final String cropBox;
    private final String modelVersion;
    private final ZoneId localTz;

    public ImageBackfillRunner(String cameraBaseUrl, String bucketName, S3Client s3Client,
                               String panosPrefix, String croppedPrefix,
                               String analysisPrefix, String manifestsPrefix,
                               String cropBox, String modelVersion) {
        this.cameraBaseUrl = cameraBaseUrl;
        this.bucketName = bucketName;
        this.s3Client = s3Client;
        this.panosPrefix = panosPrefix;
        this.croppedPrefix = croppedPrefix;
        this.analysisPrefix = analysisPrefix;
        this.manifestsPrefix = manifestsPrefix;
        this.cropBox = cropBox;
        this.modelVersion = modelVersion;
        this.localTz = ZoneId.of("America/Los_Angeles");
    }

    public void runRange(ZonedDateTime start, ZonedDateTime end, int stepMinutes,
                         boolean dryRun, int workers) {
        int windowStart = 4;
        int windowEnd = 23;
        String modelsPrefix = "models";
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
        ImageFetcher imageFetcher = new RoundshotFetcher(cameraBaseUrl);
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
                imageFetcher, s3Store, panosPrefix, croppedPrefix);
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
                log.info("DRY RUN: {}  →  {}", ts.toInstant(), timeWindow.folderPath(local));
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
                    "[--camera-base-url <url>] [--crop-box <x1,y1,x2,y2>] [--model-version <v3>] " +
                    "[--panos-prefix <prefix>] [--cropped-prefix <prefix>] " +
                    "[--analysis-prefix <prefix>] [--manifests-prefix <prefix>] " +
                    "[--workers <n>] [--dry-run]");
            System.exit(2);
        }

        String start = null, end = null, bucket = null;
        String cameraBaseUrl  = DEFAULT_CAMERA_BASE_URL;
        String cropBox        = DEFAULT_CROP_BOX;
        String modelVersion   = DEFAULT_MODEL_VERSION;
        String panosPrefix    = DEFAULT_PANOS_PREFIX;
        String croppedPrefix  = DEFAULT_CROPPED_PREFIX;
        String analysisPrefix = "analysis";
        String manifestsPrefix = "manifests";
        boolean dryRun = false;
        int workers = 1;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--start":            start         = args[++i]; break;
                case "--end":              end           = args[++i]; break;
                case "--bucket":           bucket        = args[++i]; break;
                case "--camera-base-url":  cameraBaseUrl = args[++i]; break;
                case "--crop-box":         cropBox       = args[++i]; break;
                case "--model-version":    modelVersion  = args[++i]; break;
                case "--panos-prefix":     panosPrefix   = args[++i]; break;
                case "--cropped-prefix":   croppedPrefix = args[++i]; break;
                case "--analysis-prefix":  analysisPrefix  = args[++i]; break;
                case "--manifests-prefix": manifestsPrefix = args[++i]; break;
                case "--workers":          workers       = Integer.parseInt(args[++i]); break;
                case "--dry-run":          dryRun        = true; break;
                default:
                    log.error("Unknown arg: {}", args[i]);
                    System.exit(2);
            }
        }

        if (start == null || end == null || bucket == null) {
            log.error("--start, --end and --bucket are required");
            System.exit(2);
        }

        log.info("Backfill config: camera={} cropBox={} modelVersion={} panos={} cropped={}",
                cameraBaseUrl, cropBox, modelVersion, panosPrefix, croppedPrefix);

        ZonedDateTime s = java.time.LocalDate.parse(start).atStartOfDay(java.time.ZoneOffset.UTC);
        ZonedDateTime e = java.time.LocalDate.parse(end).atTime(23, 59, 59).atZone(java.time.ZoneOffset.UTC);

        S3Client s3 = S3Client.create();
        ImageBackfillRunner runner = new ImageBackfillRunner(
                cameraBaseUrl, bucket, s3,
                panosPrefix, croppedPrefix, analysisPrefix, manifestsPrefix,
                cropBox, modelVersion);

        runner.runRange(s, e, 10, dryRun, workers);
    }
}
