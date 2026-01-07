package com.tahomatracker.service.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Builder;
import lombok.Value;
import lombok.extern.jackson.Jacksonized;
import java.util.Map;

/**
 * Structured payload for latest.json.
 */
@Value
@Builder
@Jacksonized
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class LatestSnapshot {
    @JsonProperty("image_id")
    String imageId;

    @JsonProperty("frame_state_probabilities")
    Map<String, Double> frameStateProbabilities;

    @JsonProperty("visibility_probabilities")
    Map<String, Double> visibilityProbabilities;

    @JsonProperty("frame_state_model_version")
    String frameStateModelVersion;

    @JsonProperty("visibility_model_version")
    String visibilityModelVersion;

    @JsonProperty("cropped_s3_key")
    String croppedS3Key;
    @JsonProperty("analysis_s3_key")
    String analysisS3Key;
    @JsonProperty("updated_at")
    String updatedAt;
}
