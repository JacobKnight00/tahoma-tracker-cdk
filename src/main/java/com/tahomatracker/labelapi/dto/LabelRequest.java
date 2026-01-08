package com.tahomatracker.labelapi.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;

/**
 * Request body for label submission.
 *
 * Example:
 * {
 *   "imageId": "2025/01/15/1430",
 *   "frameState": "good",
 *   "visibility": "out"
 * }
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record LabelRequest(
        @JsonProperty("imageId") String imageId,
        @JsonProperty("frameState") String frameState,
        @JsonProperty("visibility") String visibility
) {

    /**
     * Validates the request and returns any validation error, or null if valid.
     */
    public String validate() {
        if (imageId == null || imageId.isBlank()) {
            return "imageId is required";
        }

        if (!isValidImageIdFormat(imageId)) {
            return "imageId must be in format YYYY/MM/DD/HHmm";
        }

        if (frameState == null || frameState.isBlank()) {
            return "frameState is required";
        }

        FrameState parsedFrameState = FrameState.fromValue(frameState);
        if (parsedFrameState == null) {
            return "frameState must be one of: good, off_target, dark, bad";
        }

        if (parsedFrameState == FrameState.GOOD) {
            if (visibility == null || visibility.isBlank()) {
                return "visibility is required when frameState is 'good'";
            }
            Visibility parsedVisibility = Visibility.fromValue(visibility);
            if (parsedVisibility == null) {
                return "visibility must be one of: out, partially_out, not_out";
            }
        } else {
            if (visibility != null && !visibility.isBlank()) {
                return "visibility must be null when frameState is not 'good'";
            }
        }

        return null; // valid
    }

    /**
     * Returns the parsed FrameState enum.
     */
    public FrameState frameStateEnum() {
        return FrameState.fromValue(frameState);
    }

    /**
     * Returns the parsed Visibility enum (may be null).
     */
    public Visibility visibilityEnum() {
        return Visibility.fromValue(visibility);
    }

    private static boolean isValidImageIdFormat(String imageId) {
        // Format: YYYY/MM/DD/HHmm (e.g., 2025/01/15/1430)
        if (imageId.length() != 15) {
            return false;
        }
        return imageId.matches("\\d{4}/\\d{2}/\\d{2}/\\d{4}");
    }
}
