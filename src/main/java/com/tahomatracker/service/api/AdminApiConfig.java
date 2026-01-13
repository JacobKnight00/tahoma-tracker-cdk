package com.tahomatracker.service.api;

import java.util.Arrays;
import java.util.List;

/**
 * Configuration for the Admin API Lambda.
 * Loads settings from environment variables.
 */
public record AdminApiConfig(
    String bucketName,
    String imageLabelsTableName,
    String analysisPrefix,
    String modelVersion,
    List<String> allowedOrigins
) {

    public static AdminApiConfig fromEnvironment() {
        String bucketName = System.getenv("BUCKET_NAME");
        String imageLabelsTableName = System.getenv("IMAGE_LABELS_TABLE_NAME");
        String analysisPrefix = System.getenv("ANALYSIS_PREFIX");
        String modelVersion = System.getenv("MODEL_VERSION");
        String allowedOriginsStr = System.getenv("ALLOWED_ORIGINS");
        
        List<String> allowedOrigins = allowedOriginsStr != null 
            ? Arrays.asList(allowedOriginsStr.split(","))
            : List.of();

        return new AdminApiConfig(
            bucketName,
            imageLabelsTableName,
            analysisPrefix,
            modelVersion,
            allowedOrigins
        );
    }

    /**
     * Builds the S3 key for an analysis file.
     */
    public String analysisKey(String imageId) {
        return String.format("%s/%s/%s.json", analysisPrefix, modelVersion, imageId);
    }
}
