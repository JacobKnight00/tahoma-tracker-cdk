package com.tahomatracker.service.api;

import java.util.Arrays;
import java.util.List;

import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;

/**
 * Configuration for the Admin API Lambda.
 * Loads settings from environment variables.
 */
public record AdminApiConfig(
    String bucketName,
    String imageLabelsTableName,
    String analysisPrefix,
        String modelVersion,
        List<String> allowedOrigins,
        String apiSecret
) {

    public static AdminApiConfig fromEnvironment() {
        String bucketName = System.getenv("BUCKET_NAME");
        String imageLabelsTableName = System.getenv("IMAGE_LABELS_TABLE_NAME");
        String analysisPrefix = System.getenv("ANALYSIS_PREFIX");
        String modelVersion = System.getenv("MODEL_VERSION");
        String allowedOriginsStr = System.getenv("ALLOWED_ORIGINS");
        String apiSecret = resolveApiSecret();
        
        List<String> allowedOrigins = allowedOriginsStr != null 
            ? Arrays.asList(allowedOriginsStr.split(","))
            : List.of();

        return new AdminApiConfig(
            bucketName,
            imageLabelsTableName,
            analysisPrefix,
            modelVersion,
            allowedOrigins,
            apiSecret
        );
    }

    /**
     * Builds the S3 key for an analysis file.
     */
    public String analysisKey(String imageId) {
        return String.format("%s/%s/%s.json", analysisPrefix, modelVersion, imageId);
    }

    private static String resolveApiSecret() {
        String direct = System.getenv("API_SHARED_SECRET");
        if (direct != null && !direct.isBlank()) {
            return direct;
        }

        String paramName = System.getenv("API_SHARED_SECRET_PARAM");
        if (paramName == null || paramName.isBlank()) {
            return "";
        }

        try (SsmClient ssm = SsmClient.builder().build()) {
            var resp = ssm.getParameter(GetParameterRequest.builder()
                    .name(paramName)
                    .withDecryption(true)
                    .build());
            String value = resp.parameter().value();
            return value == null ? "" : value;
        } catch (Exception e) {
            return "";
        }
    }
}
