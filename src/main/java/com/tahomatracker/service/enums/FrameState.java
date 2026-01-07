package com.tahomatracker.service.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Classification labels for frame quality.
 */
public enum FrameState {
    GOOD("good"),
    OFF_TARGET("off_target"),
    DARK("dark"),
    BAD("bad");

    private final String value;

    FrameState(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static FrameState fromValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        String normalized = value.trim().toLowerCase().replace('-', '_');
        for (FrameState state : values()) {
            if (state.value.equals(normalized)) {
                return state;
            }
        }
        return null;
    }
}
