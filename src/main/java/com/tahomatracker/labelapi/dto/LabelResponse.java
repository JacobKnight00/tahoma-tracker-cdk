package com.tahomatracker.labelapi.dto;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for label submission.
 *
 * Success example:
 * {
 *   "success": true,
 *   "message": "Label recorded"
 * }
 *
 * Error example:
 * {
 *   "success": false,
 *   "error": "imageId is required"
 * }
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record LabelResponse(
        @JsonProperty("success") boolean success,
        @JsonProperty("message") String message,
        @JsonProperty("error") String error
) {

    /**
     * Creates a success response.
     */
    public static LabelResponse success(String message) {
        return new LabelResponse(true, message, null);
    }

    /**
     * Creates an error response.
     */
    public static LabelResponse error(String error) {
        return new LabelResponse(false, null, error);
    }
}
