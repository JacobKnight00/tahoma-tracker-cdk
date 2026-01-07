package com.tahomatracker.service.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Immutable crop box coordinates (x1,y1,x2,y2).
 */
public record CropBox(int x1, int y1, int x2, int y2) {
    public CropBox {
        if (x2 <= x1 || y2 <= y1) {
            throw new IllegalArgumentException("Crop box must have positive width/height");
        }
    }

    /**
     * Parses a crop box string in the format "x1,y1,x2,y2".
     * Returns null when the input is null or blank.
     */
    @JsonCreator
    public static CropBox fromString(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }

        String[] parts = value.split(",");
        if (parts.length != 4) {
            throw new IllegalArgumentException("Crop box must be in format x1,y1,x2,y2");
        }

        try {
            int x1 = Integer.parseInt(parts[0].trim());
            int y1 = Integer.parseInt(parts[1].trim());
            int x2 = Integer.parseInt(parts[2].trim());
            int y2 = Integer.parseInt(parts[3].trim());
            return new CropBox(x1, y1, x2, y2);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Crop box values must be integers", ex);
        }
    }

    /**
     * Creates a crop box that covers the entire image.
     */
    public static CropBox fullImage(int width, int height) {
        if (width <= 0 || height <= 0) {
            throw new IllegalArgumentException("Image dimensions must be positive");
        }
        return new CropBox(0, 0, width, height);
    }

    /**
     * Clamps this crop box to the given image bounds.
     */
    public CropBox clampToImage(int width, int height) {
        int clampedX1 = clamp(x1, 0, width);
        int clampedY1 = clamp(y1, 0, height);
        int clampedX2 = clamp(x2, 0, width);
        int clampedY2 = clamp(y2, 0, height);

        if (clampedX2 <= clampedX1 || clampedY2 <= clampedY1) {
            throw new IllegalArgumentException("Invalid crop box after clamping to image bounds");
        }

        return new CropBox(clampedX1, clampedY1, clampedX2, clampedY2);
    }

    public int width() {
        return x2 - x1;
    }

    public int height() {
        return y2 - y1;
    }

    @JsonValue
    @Override
    public String toString() {
        return x1 + "," + y1 + "," + x2 + "," + y2;
    }

    private static int clamp(int value, int min, int max) {
        return Math.max(min, Math.min(max, value));
    }
}
