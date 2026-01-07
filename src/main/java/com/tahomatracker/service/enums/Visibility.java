package com.tahomatracker.service.enums;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Classification labels for mountain visibility.
 */
public enum Visibility {
    OUT("out"),
    PARTIALLY_OUT("partially_out"),
    NOT_OUT("not_out");

    private final String value;

    Visibility(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static Visibility fromValue(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        String normalized = value.trim().toLowerCase().replace('-', '_');
        for (Visibility visibility : values()) {
            if (visibility.value.equals(normalized)) {
                return visibility;
            }
        }
        return null;
    }
}
