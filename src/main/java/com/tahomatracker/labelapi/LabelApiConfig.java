package com.tahomatracker.labelapi;

import java.util.List;

/**
 * Configuration for the Label API Lambda.
 *
 * Loaded from environment variables at startup.
 */
public record LabelApiConfig(
        String bucketName,
        String imageLabelsTableName,
        String analysisPrefix,
        String modelVersion,
        List<String> allowedOrigins
) {

    /**
     * Creates config from environment variables.
     */
    public static LabelApiConfig fromEnvironment() {
        return new LabelApiConfig(
                env("BUCKET_NAME", ""),
                env("IMAGE_LABELS_TABLE_NAME", "TahomaTrackerImageLabels"),
                env("ANALYSIS_PREFIX", "analysis"),
                env("MODEL_VERSION", "v1"),
                parseOrigins(env("ALLOWED_ORIGINS", "*"))
        );
    }

    /**
     * Returns the S3 key for an image's analysis JSON.
     */
    public String analysisKey(String imageId) {
        return analysisPrefix + "/" + modelVersion + "/" + imageId + ".json";
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static List<String> parseOrigins(String origins) {
        if (origins == null || origins.isBlank() || origins.equals("*")) {
            return List.of("*");
        }
        return List.of(origins.split(","));
    }
}
