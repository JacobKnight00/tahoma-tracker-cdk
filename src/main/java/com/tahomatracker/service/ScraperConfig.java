package com.tahomatracker.service;

import java.time.ZoneId;

import com.tahomatracker.service.domain.CropBox;
public final class ScraperConfig {
    public final String bucketName;
    public final String imageLabelsTableName;
    public final String cameraBaseUrl;
    public final String panosPrefix;
    public final String croppedPrefix;
    public final String analysisPrefix;
    public final String latestKey;
    public final String modelsPrefix;
    public final String frameStateModelKey;
    public final String visibilityModelKey;
    public final int modelInputWidth;
    public final int modelInputHeight;
    public final float[] normalizationMean;
    public final float[] normalizationStd;
    public final CropBox cropBox;
    public final double outThreshold;
    public final String modelVersion;
    public final ZoneId localTz;
    public final int windowStartHour;
    public final int windowEndHour;
    public final int stepMinutes;
    public final int backfillLookbackHours;

    public ScraperConfig() {
        this.bucketName = env("BUCKET_NAME", "");
        this.imageLabelsTableName = env("IMAGE_LABELS_TABLE_NAME", "");
        this.cameraBaseUrl = env("CAMERA_BASE_URL", "https://d3omclagh7m7mg.cloudfront.net/assets");
        this.panosPrefix = env("PANOS_PREFIX", "needle-cam/panos");
        this.croppedPrefix = env("CROPPED_PREFIX", "needle-cam/cropped-images");
        this.analysisPrefix = env("ANALYSIS_PREFIX", "analysis");
        this.latestKey = env("LATEST_KEY", "latest/latest.json");
        this.modelsPrefix = env("MODELS_PREFIX", "models");
        this.cropBox = CropBox.fromString(env("CROP_BOX", ""));
        this.outThreshold = Double.parseDouble(env("OUT_THRESHOLD", "0.85"));
        this.modelVersion = env("MODEL_VERSION", "v1");
        this.frameStateModelKey = env("FRAME_STATE_MODEL_KEY", modelsPrefix + "/" + modelVersion + "/frame_state_" + modelVersion + ".onnx");
        this.visibilityModelKey = env("VISIBILITY_MODEL_KEY", modelsPrefix + "/" + modelVersion + "/visibility_" + modelVersion + ".onnx");
        this.modelInputWidth = Integer.parseInt(env("MODEL_INPUT_WIDTH", "224"));
        this.modelInputHeight = Integer.parseInt(env("MODEL_INPUT_HEIGHT", "224"));
        this.normalizationMean = parseFloatArray(env("MODEL_NORMALIZATION_MEAN", "0.485,0.456,0.406"));
        this.normalizationStd = parseFloatArray(env("MODEL_NORMALIZATION_STD", "0.229,0.224,0.225"));
        this.localTz = ZoneId.of(env("LOCAL_TZ", "America/Los_Angeles"));
        this.windowStartHour = Integer.parseInt(env("WINDOW_START_HOUR", "4"));
        this.windowEndHour = Integer.parseInt(env("WINDOW_END_HOUR", "23"));
        this.stepMinutes = Integer.parseInt(env("STEP_MINUTES", "10"));
        this.backfillLookbackHours = Integer.parseInt(env("BACKFILL_LOOKBACK_HOURS", "6"));
    }

    // helper: allow tests to create config with overrides
    public ScraperConfig(String bucketName, String imageLabelsTableName, String cameraBaseUrl, String panosPrefix,
                         String croppedPrefix, String analysisPrefix, String latestKey, CropBox cropBox,
                         double outThreshold, String modelVersion, ZoneId localTz, int windowStartHour,
                         int windowEndHour, int stepMinutes, int backfillLookbackHours,
                         String modelsPrefix, String frameStateModelKey, String visibilityModelKey,
                         int modelInputWidth, int modelInputHeight,
                         float[] normalizationMean, float[] normalizationStd) {
        this.bucketName = bucketName;
        this.imageLabelsTableName = imageLabelsTableName;
        this.cameraBaseUrl = cameraBaseUrl;
        this.panosPrefix = panosPrefix;
        this.croppedPrefix = croppedPrefix;
        this.analysisPrefix = analysisPrefix;
        this.latestKey = latestKey;
        this.modelsPrefix = modelsPrefix;
        this.frameStateModelKey = frameStateModelKey;
        this.visibilityModelKey = visibilityModelKey;
        this.modelInputWidth = modelInputWidth;
        this.modelInputHeight = modelInputHeight;
        this.normalizationMean = normalizationMean;
        this.normalizationStd = normalizationStd;
        this.cropBox = cropBox;
        this.outThreshold = outThreshold;
        this.modelVersion = modelVersion;
        this.localTz = localTz;
        this.windowStartHour = windowStartHour;
        this.windowEndHour = windowEndHour;
        this.stepMinutes = stepMinutes;
        this.backfillLookbackHours = backfillLookbackHours;
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static float[] parseFloatArray(String csv) {
        String[] parts = csv.split(",");
        float[] arr = new float[parts.length];
        for (int i = 0; i < parts.length; i++) {
            arr[i] = Float.parseFloat(parts[i].trim());
        }
        return arr;
    }
}
