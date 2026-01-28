package com.tahomatracker.service.api;

import java.util.List;

import software.amazon.awssdk.services.ssm.SsmClient;
import software.amazon.awssdk.services.ssm.model.GetParameterRequest;

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
        List<String> allowedOrigins,
        String apiSecret
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
                parseOrigins(env("ALLOWED_ORIGINS", "*")),
                resolveApiSecret()
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

    private static String resolveApiSecret() {
        String direct = env("API_SHARED_SECRET", "");
        if (direct != null && !direct.isBlank()) {
            return direct;
        }

        String paramName = env("API_SHARED_SECRET_PARAM", "");
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
