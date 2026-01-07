package com.tahomatracker.service;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.tahomatracker.backfill.FastSliceFetcher;
import com.tahomatracker.service.classifier.OnnxFrameStateClassifier;
import com.tahomatracker.service.classifier.OnnxModelLoader;
import com.tahomatracker.service.classifier.OnnxVisibilityClassifier;
import com.tahomatracker.service.process.AnalysisPersistenceService;
import com.tahomatracker.service.process.ImageAcquisitionService;
import com.tahomatracker.service.process.ImageClassificationService;
import com.tahomatracker.service.process.LatestImageService;
import com.tahomatracker.service.process.PipelineRunner;
import com.tahomatracker.service.process.TimeWindowPlanner;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.S3ObjectStorageClient;
import com.tahomatracker.service.external.SliceFetcher;
import org.slf4j.MDC;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Lambda handler for Tahoma Tracker image scraping and processing.
 */
@Slf4j
public class ScraperHandler implements RequestHandler<Map<String, Object>, Map<String, Object>> {

    private final PipelineRunner runner;

    public ScraperHandler() {
        this(new ScraperConfig());
    }

    public ScraperHandler(ScraperConfig config) {
        this(config, S3Client.builder().build());
    }

    private ScraperHandler(ScraperConfig config, S3Client s3Client) {
        this(new FastSliceFetcher(config.cameraBaseUrl, 4),
                new S3ObjectStorageClient(s3Client, config.bucketName),
                config,
                s3Client);
    }

    // Constructor for integration tests and DI
    public ScraperHandler(SliceFetcher fetcher, ObjectStorageClient s3Store, ScraperConfig config) {
        this(fetcher, s3Store, config, S3Client.builder().build());
    }

    // Internal constructor allowing shared S3 client for model downloads
    public ScraperHandler(SliceFetcher fetcher, ObjectStorageClient s3Store, ScraperConfig config, S3Client s3Client) {
        // Initialize services
        OnnxModelLoader modelLoader = new OnnxModelLoader(s3Client, config.bucketName);
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
                fetcher, s3Store, config.panosPrefix, config.croppedPrefix);
        ImageClassificationService classification = new ImageClassificationService(
                frameStateClassifier, visibilityClassifier, s3Store);
        AnalysisPersistenceService persistence = new AnalysisPersistenceService(s3Store, config.analysisPrefix, config.modelVersion);

        var timeWindow = new TimeWindowPlanner(config);
        var latestService = new LatestImageService(s3Store, config.latestKey);
        this.runner = new PipelineRunner(config, timeWindow, latestService, imageAcquisition,
                classification, persistence, s3Store);
    }

    @Override
    public Map<String, Object> handleRequest(Map<String, Object> event, Context context) {
        String requestId = context != null ? context.getAwsRequestId() : null;
        if (requestId != null) {
            MDC.put("aws_request_id", requestId);
        }
        try {
            log.info("Starting pipeline invocation");
            return runner.run(context);

        } catch (IOException | InterruptedException ex) {
            log.error("I/O error: {}", ex.getMessage(), ex);
            return buildErrorResponse(ex.getMessage());
        } catch (Exception ex) {
            log.error("Pipeline failed", ex);
            return buildErrorResponse(ex.getMessage());
        } finally {
            MDC.clear();
        }
    }

    private Map<String, Object> buildErrorResponse(String error) {
        Map<String, Object> response = new HashMap<>();
        response.put("timestamp", isoformat(Instant.now()));
        response.put("status", "error");
        response.put("error", error);
        return response;
    }

    private String isoformat(Instant instant) {
        return instant.atOffset(ZoneOffset.UTC)
                .withNano(0)
                .toString()
                .replace("+00:00", "Z");
    }
}
