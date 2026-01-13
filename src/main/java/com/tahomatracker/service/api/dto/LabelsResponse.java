package com.tahomatracker.service.api.dto;

import java.util.List;

/**
 * Response for GET /admin/labels endpoint.
 */
public record LabelsResponse(
    boolean success,
    List<LabelDto> labels,
    int count,
    String error
) {
    public static LabelsResponse success(List<LabelDto> labels) {
        return new LabelsResponse(true, labels, labels.size(), null);
    }

    public static LabelsResponse error(String error) {
        return new LabelsResponse(false, List.of(), 0, error);
    }
}
