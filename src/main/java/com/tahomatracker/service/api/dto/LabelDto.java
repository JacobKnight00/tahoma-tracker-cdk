package com.tahomatracker.service.api.dto;

/**
 * DTO representing a single label in API responses.
 */
public record LabelDto(
    String imageId,
    String frameState,
    String visibility,
    String labelSource,
    String updatedBy,
    String updatedAt
) {}
