package com.tahomatracker.service.domain;

import java.time.format.DateTimeFormatter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
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
    private static final DateTimeFormatter IMAGE_ID_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd/HHmm");

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
     * Converts this ImageId (interpreted in the provided local timezone) to an ISO-8601 UTC timestamp.
     */
    public String toIsoTimestampUtc(ZoneId localTz) {
        return DateTimeFormatter.ISO_INSTANT.format(toInstant(localTz));
    }

    /**
     * Converts this ImageId to a ZonedDateTime in the provided local timezone.
     */
    public ZonedDateTime toZonedDateTime(ZoneId localTz) {
        LocalDateTime local = LocalDateTime.parse(value, IMAGE_ID_FORMATTER);
        return local.atZone(localTz);
    }

    /**
     * Converts this ImageId (interpreted in the provided local timezone) to an Instant in UTC.
     */
    public Instant toInstant(ZoneId localTz) {
        return toZonedDateTime(localTz).toInstant();
    }

    /**
     * Creates an ImageId from a local ZonedDateTime.
     */
    public static ImageId fromZonedDateTime(ZonedDateTime localTs) {
        return parse(IMAGE_ID_FORMATTER.format(localTs));
    }

    /**
     * Creates an ImageId from a UTC ZonedDateTime by converting to the provided local timezone.
     */
    public static ImageId fromUtc(ZonedDateTime utcTs, ZoneId localTz) {
        return fromZonedDateTime(utcTs.withZoneSameInstant(localTz));
    }
}
