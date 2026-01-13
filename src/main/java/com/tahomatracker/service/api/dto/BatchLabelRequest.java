package com.tahomatracker.service.api.dto;

import java.util.List;

/**
 * Request for batch label submission.
 */
public record BatchLabelRequest(
    List<SingleLabelRequest> labels,
    String updatedBy
) {
    public String validate() {
        if (labels == null || labels.isEmpty()) {
            return "labels array is required and cannot be empty";
        }

        if (labels.size() > 50) {
            return "Maximum 50 labels per batch";
        }

        for (int i = 0; i < labels.size(); i++) {
            SingleLabelRequest label = labels.get(i);
            String error = label.validate();
            if (error != null) {
                return "Label " + i + ": " + error;
            }
        }

        return null;
    }

    public record SingleLabelRequest(
        String imageId,
        String frameState,
        String visibility
    ) {
        public String validate() {
            if (imageId == null || imageId.isBlank()) {
                return "imageId is required";
            }

            if (!imageId.matches("\\d{4}/\\d{2}/\\d{2}/\\d{4}")) {
                return "imageId must be in format YYYY/MM/DD/HHMM";
            }

            if (frameState == null || frameState.isBlank()) {
                return "frameState is required";
            }

            if (!List.of("good", "dark", "bad", "off_target").contains(frameState)) {
                return "frameState must be one of: good, dark, bad, off_target";
            }

            if ("good".equals(frameState)) {
                if (visibility == null || visibility.isBlank()) {
                    return "visibility is required when frameState is good";
                }
                if (!List.of("out", "partially_out", "not_out").contains(visibility)) {
                    return "visibility must be one of: out, partially_out, not_out";
                }
            } else if (visibility != null && !visibility.isBlank()) {
                return "visibility should not be set when frameState is not good";
            }

            return null;
        }
    }
}
