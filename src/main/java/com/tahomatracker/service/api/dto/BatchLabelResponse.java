package com.tahomatracker.service.api.dto;

import java.util.List;

/**
 * Response for batch label submission.
 */
public record BatchLabelResponse(
    boolean success,
    int processed,
    int failed,
    List<String> errors,
    String message
) {
    public static BatchLabelResponse success(int processed) {
        return new BatchLabelResponse(true, processed, 0, List.of(), 
                                    "Successfully processed " + processed + " labels");
    }

    public static BatchLabelResponse error(String message) {
        return new BatchLabelResponse(false, 0, 0, List.of(), message);
    }

    public static BatchLabelResponse error(String message, List<String> errors) {
        return new BatchLabelResponse(false, 0, errors.size(), errors, message);
    }
}
