package com.tahomatracker.service.domain;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import com.tahomatracker.service.enums.AcquisitionStatus;
import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;

/**
 * Tracks image data and analysis results as they flow through the processing pipeline.
 * Contains only the essential data needed for processing and persistence.
 */
@Data
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ImageContext {
    // Image identifier (yyyy/MM/dd/HHmm)
    @JsonProperty("image_id")
    private String imageId;

    // Crop coordinates (x1,y1,x2,y2)
    @JsonProperty("crop_box")
    private CropBox cropBox;

    // S3 locations
    @JsonProperty("pano_s3_key")
    private String panoS3Key;

    @JsonProperty("cropped_s3_key")
    private String croppedS3Key;

    @JsonProperty("analysis_s3_key")
    private String analysisS3Key;

    // Top classes (for runtime decisioning; not persisted in analysis map)
    private FrameState frameState;
    private Double frameStateProb;
    private Visibility visibility;
    private Double visibilityProb;

    // Probability maps (softmax outputs)
    @JsonProperty("frame_state_probabilities")
    private Map<String, Double> frameStateProbabilities;

    @JsonProperty("visibility_probabilities")
    private Map<String, Double> visibilityProbabilities;

    // Processing metadata
    private AcquisitionStatus status;

    @JsonProperty("frame_state_model_version")
    private String frameStateModelVersion;

    @JsonProperty("visibility_model_version")
    private String visibilityModelVersion;

    @JsonProperty("updated_at")
    private String updatedAt;

    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("image_id", imageId);
        map.put("frame_state_probabilities", frameStateProbabilities);
        map.put("visibility_probabilities", visibilityProbabilities);
        map.put("frame_state_model_version", frameStateModelVersion);
        map.put("visibility_model_version", visibilityModelVersion);
        map.put("cropped_s3_key", croppedS3Key);
        map.put("analysis_s3_key", analysisS3Key);
        map.put("pano_s3_key", panoS3Key);
        map.put("updated_at", updatedAt);
        return map;
    }

}
