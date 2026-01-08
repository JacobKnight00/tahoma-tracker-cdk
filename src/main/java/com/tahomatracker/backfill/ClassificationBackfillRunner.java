package com.tahomatracker.backfill;

import com.tahomatracker.service.ScraperConfig;
import com.tahomatracker.service.classifier.OnnxFrameStateClassifier;
import com.tahomatracker.service.classifier.OnnxModelLoader;
import com.tahomatracker.service.classifier.OnnxVisibilityClassifier;
import com.tahomatracker.service.domain.ClassificationResult;
import com.tahomatracker.service.domain.CropBox;
import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.domain.ImageId;
import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.S3ObjectStorageClient;
import com.tahomatracker.service.process.AnalysisPersistenceService;
import com.tahomatracker.service.process.ImageClassificationService;
import com.tahomatracker.service.process.TimeWindowPlanner;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CLI tool to run classification on existing cropped images in S3.
 * Useful for spot-checking model performance and backfilling classification data.
 */
public class ClassificationBackfillRunner {
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(ClassificationBackfillRunner.class);

    private final String bucketName;
    private final S3Client s3Client;
    private final String panosPrefix;
    private final String croppedPrefix;
    private final String analysisPrefix;
    private final String modelVersion;
    private final ZoneId localTz;

    public ClassificationBackfillRunner(String bucketName, S3Client s3Client,
                                        String panosPrefix, String croppedPrefix,
                                        String analysisPrefix, String modelVersion) {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
        this.panosPrefix = panosPrefix;
        this.croppedPrefix = croppedPrefix;
        this.analysisPrefix = analysisPrefix;
        this.modelVersion = modelVersion;
        this.localTz = ZoneId.of("America/Los_Angeles");
    }

    /**
     * Run classification on specific image IDs (spot-check mode).
     */
    public void runOnImages(List<String> imageIds, boolean dryRun) {
        ObjectStorageClient s3Store = new S3ObjectStorageClient(s3Client, bucketName);
        ImageClassificationService classification = createClassificationService(s3Store);
        AnalysisPersistenceService persistence = new AnalysisPersistenceService(s3Store, analysisPrefix, modelVersion);

        int processed = 0;
        int skipped = 0;
        int errors = 0;

        for (String imageId : imageIds) {
            try {
                ClassificationBackfillResult result = processImage(imageId, s3Store, classification, persistence, dryRun);
                if (result.skipped) {
                    skipped++;
                } else {
                    processed++;
                }
                printResult(imageId, result);
            } catch (Exception e) {
                log.error("Error processing {}: {}", imageId, e.getMessage(), e);
                errors++;
            }
        }

        System.out.println();
        System.out.printf("Summary: %d processed, %d skipped, %d errors%n", processed, skipped, errors);
    }

    /**
     * Run classification on a date range (bulk mode).
     */
    public void runOnRange(ZonedDateTime start, ZonedDateTime end, int stepMinutes, int workers, boolean dryRun) {
        // Generate image IDs for the date range
        List<String> imageIds = generateImageIds(start, end, stepMinutes);
        log.info("Generated {} candidate timestamps from {} to {}", imageIds.size(), start, end);

        if (workers <= 1) {
            runOnImages(imageIds, dryRun);
            return;
        }

        // Multi-threaded execution
        ObjectStorageClient s3Store = new S3ObjectStorageClient(s3Client, bucketName);
        ImageClassificationService classification = createClassificationService(s3Store);
        AnalysisPersistenceService persistence = new AnalysisPersistenceService(s3Store, analysisPrefix, modelVersion);

        ExecutorService pool = Executors.newFixedThreadPool(workers);
        CompletionService<ClassificationBackfillResult> completion = new ExecutorCompletionService<>(pool);

        AtomicInteger processed = new AtomicInteger(0);
        AtomicInteger skipped = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);

        try {
            for (String imageId : imageIds) {
                completion.submit(() -> {
                    try {
                        return processImage(imageId, s3Store, classification, persistence, dryRun);
                    } catch (Exception e) {
                        ClassificationBackfillResult errorResult = new ClassificationBackfillResult();
                        errorResult.error = e.getMessage();
                        return errorResult;
                    }
                });
            }

            for (int i = 0; i < imageIds.size(); i++) {
                try {
                    ClassificationBackfillResult result = completion.take().get();
                    if (result.error != null) {
                        errors.incrementAndGet();
                    } else if (result.skipped) {
                        skipped.incrementAndGet();
                    } else {
                        processed.incrementAndGet();
                    }
                } catch (ExecutionException ex) {
                    log.error("Worker failed", ex.getCause());
                    errors.incrementAndGet();
                }
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdown();
        }

        System.out.println();
        System.out.printf("Summary: %d processed, %d skipped, %d errors%n",
                processed.get(), skipped.get(), errors.get());
    }

    private ImageClassificationService createClassificationService(ObjectStorageClient s3Store) {
        String modelsPrefix = "models";
        String frameStateModelKey = modelsPrefix + "/" + modelVersion + "/frame_state_" + modelVersion + ".onnx";
        String visibilityModelKey = modelsPrefix + "/" + modelVersion + "/visibility_" + modelVersion + ".onnx";
        float[] mean = new float[]{0.485f, 0.456f, 0.406f};
        float[] std = new float[]{0.229f, 0.224f, 0.225f};

        OnnxModelLoader modelLoader = new OnnxModelLoader(s3Client, bucketName);
        var frameStateClassifier = new OnnxFrameStateClassifier(
                modelLoader, frameStateModelKey, 224, 224, mean, std);
        var visibilityClassifier = new OnnxVisibilityClassifier(
                modelLoader, visibilityModelKey, 224, 224, mean, std);

        return new ImageClassificationService(frameStateClassifier, visibilityClassifier, s3Store);
    }

    private ClassificationBackfillResult processImage(String imageId,
                                                       ObjectStorageClient s3Store,
                                                       ImageClassificationService classification,
                                                       AnalysisPersistenceService persistence,
                                                       boolean dryRun) throws IOException {
        ClassificationBackfillResult result = new ClassificationBackfillResult();
        ImageId id = ImageId.parse(imageId);
        String idValue = id.getValue();
        result.imageId = idValue;

        String croppedKey = croppedPrefix + "/" + idValue + ".jpg";
        String panoKey = panosPrefix + "/" + idValue + ".jpg";

        // Check if cropped image exists
        if (!s3Store.exists(croppedKey)) {
            result.skipped = true;
            result.skipReason = "cropped image not found: " + croppedKey;
            return result;
        }

        // Run classification
        ClassificationResult classResult = classification.classifyFromS3(croppedKey);
        result.classificationResult = classResult;

        if (dryRun) {
            result.analysisKey = persistence.formatAnalysisKey(id) + " (dry run)";
            return result;
        }

        // Build ImageContext and persist
        ImageContext context = new ImageContext();
        context.setImageId(idValue);
        context.setCroppedS3Key(croppedKey);
        context.setPanoS3Key(panoKey);
        context.setFrameState(classResult.getFrameState());
        context.setFrameStateProb(classResult.getFrameStateProb());
        context.setVisibility(classResult.getVisibility());
        context.setVisibilityProb(classResult.getVisibilityProb());
        context.setFrameStateProbabilities(toStringMap(classResult.getFrameStateProbabilities()));
        context.setVisibilityProbabilities(toStringMap(classResult.getVisibilityProbabilities()));
        context.setFrameStateModelVersion(modelVersion);
        context.setVisibilityModelVersion(modelVersion);
        context.setUpdatedAt(Instant.now().toString());

        result.analysisKey = persistence.persistAnalysis(context, id);
        return result;
    }

    private List<String> generateImageIds(ZonedDateTime start, ZonedDateTime end, int stepMinutes) {
        List<String> ids = new ArrayList<>();
        int windowStart = 4;
        int windowEnd = 23;

        ZonedDateTime ts = start;
        while (!ts.isAfter(end)) {
            ZonedDateTime local = ts.withZoneSameInstant(localTz);
            if (local.getHour() >= windowStart && local.getHour() < windowEnd) {
                ids.add(ImageId.fromZonedDateTime(local).getValue());
            }
            ts = ts.plusMinutes(stepMinutes);
        }
        return ids;
    }

    private void printResult(String imageId, ClassificationBackfillResult result) {
        System.out.println();
        System.out.printf("Processing %s...%n", imageId);

        if (result.skipped) {
            System.out.printf("  SKIPPED: %s%n", result.skipReason);
            return;
        }

        if (result.error != null) {
            System.out.printf("  ERROR: %s%n", result.error);
            return;
        }

        ClassificationResult cr = result.classificationResult;
        if (cr != null) {
            FrameState fs = cr.getFrameState();
            System.out.printf("  Frame State: %s (%.2f)%n", fs != null ? fs.getValue() : "null", cr.getFrameStateProb());
            if (cr.getFrameStateProbabilities() != null) {
                StringBuilder probs = new StringBuilder("    ");
                cr.getFrameStateProbabilities().forEach((k, v) ->
                        probs.append(String.format("%s=%.2f, ", k.getValue(), v)));
                System.out.println(probs.toString().replaceAll(", $", ""));
            }

            if (fs == FrameState.GOOD && cr.getVisibility() != null) {
                System.out.printf("  Visibility: %s (%.2f)%n", cr.getVisibility().getValue(), cr.getVisibilityProb());
                if (cr.getVisibilityProbabilities() != null) {
                    StringBuilder probs = new StringBuilder("    ");
                    cr.getVisibilityProbabilities().forEach((k, v) ->
                            probs.append(String.format("%s=%.2f, ", k.getValue(), v)));
                    System.out.println(probs.toString().replaceAll(", $", ""));
                }
            } else if (fs != FrameState.GOOD) {
                System.out.println("  Visibility: (skipped - frame not GOOD)");
            }
        }

        System.out.printf("  → Analysis written to %s%n", result.analysisKey);
    }

    private Map<String, Double> toStringMap(Map<?, Double> probs) {
        if (probs == null) return null;
        Map<String, Double> out = new HashMap<>();
        probs.forEach((k, v) -> {
            if (k instanceof FrameState) {
                out.put(((FrameState) k).getValue(), v);
            } else if (k instanceof Visibility) {
                out.put(((Visibility) k).getValue(), v);
            } else if (k != null) {
                out.put(k.toString(), v);
            }
        });
        return out;
    }

    private static class ClassificationBackfillResult {
        String imageId;
        boolean skipped;
        String skipReason;
        String error;
        ClassificationResult classificationResult;
        String analysisKey;
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(2);
        }

        String bucket = null;
        String imagesArg = null;
        String start = null;
        String end = null;
        boolean dryRun = false;
        int workers = 1;
        String modelVersion = "v1";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bucket": bucket = args[++i]; break;
                case "--images": imagesArg = args[++i]; break;
                case "--start": start = args[++i]; break;
                case "--end": end = args[++i]; break;
                case "--dry-run": dryRun = true; break;
                case "--workers": workers = Integer.parseInt(args[++i]); break;
                case "--model-version": modelVersion = args[++i]; break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
                default:
                    log.error("Unknown arg: {}", args[i]);
                    printUsage();
                    System.exit(2);
            }
        }

        if (bucket == null) {
            log.error("--bucket is required");
            System.exit(2);
        }

        if (imagesArg == null && (start == null || end == null)) {
            log.error("Either --images or both --start and --end are required");
            System.exit(2);
        }

        S3Client s3 = S3Client.create();
        ClassificationBackfillRunner runner = new ClassificationBackfillRunner(
                bucket, s3,
                "needle-cam/panos",
                "needle-cam/cropped-images",
                "analysis",
                modelVersion
        );

        if (imagesArg != null) {
            // Spot-check mode: specific image IDs
            List<String> imageIds = List.of(imagesArg.split(","));
            runner.runOnImages(imageIds, dryRun);
        } else {
            // Range mode: date range
            ZonedDateTime s = ZonedDateTime.parse(start);
            ZonedDateTime e = ZonedDateTime.parse(end);
            runner.runOnRange(s, e, 10, workers, dryRun);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: ClassificationBackfillRunner [options]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --bucket <name>       S3 bucket name (required)");
        System.out.println("  --images <ids>        Comma-separated image IDs (e.g., 2024/12/25/1200,2024/12/25/1400)");
        System.out.println("  --start <ISO>         Start timestamp (e.g., 2024-12-25T00:00:00Z)");
        System.out.println("  --end <ISO>           End timestamp (e.g., 2024-12-26T00:00:00Z)");
        System.out.println("  --workers <n>         Number of parallel workers (default: 1)");
        System.out.println("  --model-version <v>   Model version to use (default: v1)");
        System.out.println("  --dry-run             Don't write analysis files, just show classification results");
        System.out.println("  --help                Show this help");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  # Spot-check specific images");
        System.out.println("  java -cp target/tahomacdk-0.1.jar com.tahomatracker.backfill.ClassificationBackfillRunner \\");
        System.out.println("    --bucket my-bucket --images 2024/12/25/1200,2024/12/25/1400");
        System.out.println();
        System.out.println("  # Process a date range");
        System.out.println("  java -cp target/tahomacdk-0.1.jar com.tahomatracker.backfill.ClassificationBackfillRunner \\");
        System.out.println("    --bucket my-bucket --start 2024-12-25T00:00:00Z --end 2024-12-26T00:00:00Z --workers 4");
    }
}
