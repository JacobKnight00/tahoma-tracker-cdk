package com.tahomatracker.service.domain;

import java.util.regex.Pattern;

import lombok.EqualsAndHashCode;

/**
 * Value object representing an image identifier.
 *
 * Format: "YYYY/MM/DD/HHmm"
 * Example: "2024/12/23/0400"
 *
 * This ensures type safety and validation across the codebase. The underlying
 * storage in DynamoDB remains a String, but all domain-layer code uses this
 * typed wrapper for clarity and correctness.
 */
@EqualsAndHashCode
public class ImageId {

    private static final Pattern IMAGE_ID_PATTERN = Pattern.compile("^\\d{4}/\\d{2}/\\d{2}/\\d{4}$");

    private final String value;

    private ImageId(String value) {
        this.value = value;
    }

    /**
     * Creates an ImageId from a string, validating the format.
     *
     * @param value the image identifier string
     * @return the ImageId
     * @throws IllegalArgumentException if format is invalid
     */
    public static ImageId parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("ImageId cannot be null or blank");
        }
        if (!IMAGE_ID_PATTERN.matcher(value).matches()) {
            throw new IllegalArgumentException(
                "ImageId must be in format YYYY/MM/DD/HHmm, got: " + value);
        }
        return new ImageId(value);
    }

    /**
     * Creates an ImageId without validation (use with caution).
     * Prefer {@link #parse(String)} for user input.
     */
    public static ImageId of(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("ImageId cannot be null or blank");
        }
        return new ImageId(value);
    }

    /**
     * Returns the string value for storage or transmission.
     */
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

    /**
     * Extracts components from this ImageId.
     * Example: "2024/12/23/0400" -> ["2024", "12", "23", "0400"]
     */
    public String[] getParts() {
        return value.split("/");
    }

    /**
     * Gets the year component.
     */
    public String getYear() {
        return getParts()[0];
    }

    /**
     * Gets the month component.
     */
    public String getMonth() {
        return getParts()[1];
    }

    /**
     * Gets the day component.
     */
    public String getDay() {
        return getParts()[2];
    }

    /**
     * Gets the time component (HHmm).
     */
    public String getTime() {
        return getParts()[3];
    }

    /**
     * Converts this ImageId to ISO 8601 timestamp.
     * Example: "2024/12/23/0400" -> "2024-12-23T04:00:00Z"
     */
    public String toIsoTimestamp() {
        String[] parts = getParts();
        String hhmm = parts[3];
        String hour = hhmm.substring(0, 2);
        String minute = hhmm.substring(2, 4);
        return String.format("%s-%s-%sT%s:%s:00Z", parts[0], parts[1], parts[2], hour, minute);
    }

    /**
     * Creates an ImageId from ISO 8601 timestamp.
     * Example: "2024-12-23T04:00:00Z" -> "2024/12/23/0400"
     *
     * @param isoTimestamp ISO 8601 timestamp string
     * @return the ImageId
     * @throws IllegalArgumentException if timestamp format is invalid
     */
    public static ImageId fromIsoTimestamp(String isoTimestamp) {
        if (isoTimestamp == null || isoTimestamp.length() < 16) {
            throw new IllegalArgumentException("Invalid ISO timestamp: " + isoTimestamp);
        }
        // Format: YYYY-MM-DDTHH:MM:SS...
        String year = isoTimestamp.substring(0, 4);
        String month = isoTimestamp.substring(5, 7);
        String day = isoTimestamp.substring(8, 10);
        String hour = isoTimestamp.substring(11, 13);
        String minute = isoTimestamp.substring(14, 16);
        return parse(String.format("%s/%s/%s/%s%s", year, month, day, hour, minute));
    }
}
