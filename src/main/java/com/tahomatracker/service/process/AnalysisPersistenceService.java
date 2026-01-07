package com.tahomatracker.service.process;

import com.tahomatracker.service.domain.ImageContext;
import com.tahomatracker.service.external.ObjectStorageClient;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Service for persisting analysis results to S3.
 */
public class AnalysisPersistenceService {
    private final ObjectStorageClient s3Store;
    private final String analysisPrefix;
    private final String modelVersion;

    public AnalysisPersistenceService(ObjectStorageClient s3Store, String analysisPrefix, String modelVersion) {
        this.s3Store = s3Store;
        this.analysisPrefix = analysisPrefix;
        this.modelVersion = modelVersion;
    }

    /**
     * Persists analysis results to S3 as JSON.
     *
     * @param context The image context containing all analysis data
     * @param keyBase The key base in format yyyy/MM/dd/HHmm
     * @return The S3 key of the persisted analysis JSON
     */
    public String persistAnalysis(ImageContext context, String keyBase) throws IOException {
        String analysisKey = formatAnalysisKey(keyBase);

        Map<String, Object> analysis = new HashMap<>();
        analysis.put("image_id", context.getImageId());
        analysis.put("frame_state_probabilities", context.getFrameStateProbabilities());
        analysis.put("visibility_probabilities", context.getVisibilityProbabilities());
        analysis.put("frame_state_model_version", context.getFrameStateModelVersion());
        analysis.put("visibility_model_version", context.getVisibilityModelVersion());
        analysis.put("cropped_s3_key", context.getCroppedS3Key());
        analysis.put("pano_s3_key", context.getPanoS3Key());
        analysis.put("updated_at", Instant.now().toString());

        s3Store.putJson(analysisKey, analysis);

        return analysisKey;
    }

    /**
     * Formats the S3 key for an analysis JSON file.
     * Format: {analysisPrefix}/{modelVersion}/{keyBase}.json
     *
     * @param keyBase The key base in format yyyy/MM/dd/HHmm
     * @return The full S3 key
     */
    public String formatAnalysisKey(String keyBase) {
        return analysisPrefix + "/" + modelVersion + "/" + keyBase + ".json";
    }
}
