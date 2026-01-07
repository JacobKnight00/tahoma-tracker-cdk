package com.tahomatracker.service.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Status of image acquisition operation.
 */
public enum AcquisitionStatus {
    /** Images successfully fetched and uploaded */
    OK,

    /** No slices found (camera may not have published images yet) */
    IMAGES_NOT_FOUND,

    /** Analysis already exists for this timestamp */
    ALREADY_PROCESSED;

    @JsonValue
    public String toValue() {
        return name();
    }

    @JsonCreator
    public static AcquisitionStatus fromValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        String normalized = value.trim().toUpperCase();
        for (AcquisitionStatus status : values()) {
            if (status.name().equalsIgnoreCase(normalized)) {
                return status;
            }
        }
        return null;
    }
}
