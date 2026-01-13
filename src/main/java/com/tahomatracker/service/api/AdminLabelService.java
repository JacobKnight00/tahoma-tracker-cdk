package com.tahomatracker.service.api;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.tahomatracker.service.api.dto.BatchLabelRequest;
import com.tahomatracker.service.api.dto.BatchLabelResponse;
import com.tahomatracker.service.api.dto.LabelDto;
import com.tahomatracker.service.api.dto.LabelsResponse;
import com.tahomatracker.service.domain.ImageLabel;
import com.tahomatracker.service.external.LabelRepository;

import lombok.extern.slf4j.Slf4j;

/**
 * Service handling admin label operations.
 *
 * Provides functionality for:
 * - Retrieving labels for date ranges with filtering
 * - Batch submission of admin labels
 * - Date range validation and image ID generation
 */
@Slf4j
@Singleton
public class AdminLabelService {

    private static final String LABEL_SOURCE_ADMIN = "admin";
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final LabelRepository labelRepository;
    private final AdminApiConfig config;

    @Inject
    public AdminLabelService(LabelRepository labelRepository, AdminApiConfig config) {
        this.labelRepository = labelRepository;
        this.config = config;
    }

    /**
     * Retrieves labels for a date range with optional filtering.
     */
    public LabelsResponse getLabelsForDateRange(String startDate, String endDate, 
                                               String excludeLabeled, String labelSource) {
        
        // Validate date format
        LocalDate start, end;
        try {
            start = LocalDate.parse(startDate, DATE_FORMATTER);
            end = LocalDate.parse(endDate, DATE_FORMATTER);
        } catch (DateTimeParseException e) {
            log.warn("Invalid date format: startDate={}, endDate={}", startDate, endDate);
            return LabelsResponse.error("Invalid date format. Use YYYY-MM-DD");
        }

        if (start.isAfter(end)) {
            return LabelsResponse.error("startDate must be before or equal to endDate");
        }

        try {
            List<ImageLabel> labels = getLabelsInDateRange(start, end);
            
            // Apply filtering
            if (labelSource != null && !labelSource.isBlank()) {
                labels = labels.stream()
                    .filter(label -> labelSource.equals(label.getLabelSource()))
                    .collect(Collectors.toList());
            }

            // Convert to DTOs
            List<LabelDto> labelDtos = labels.stream()
                .map(this::toLabelDto)
                .collect(Collectors.toList());

            log.info("Retrieved {} labels for date range {}-{}", labelDtos.size(), startDate, endDate);
            return LabelsResponse.success(labelDtos);

        } catch (Exception e) {
            log.error("Failed to retrieve labels for date range {}-{}: {}", 
                     startDate, endDate, e.getMessage(), e);
            return LabelsResponse.error("Failed to retrieve labels");
        }
    }

    /**
     * Submits multiple admin labels in a batch operation.
     */
    public BatchLabelResponse submitBatchLabels(BatchLabelRequest request) {
        String validationError = request.validate();
        if (validationError != null) {
            log.info("Batch validation failed: {}", validationError);
            return BatchLabelResponse.error(validationError);
        }

        List<ImageLabel> labels = new ArrayList<>();
        List<String> errors = new ArrayList<>();

        // Convert request labels to domain objects
        for (var labelRequest : request.labels()) {
            try {
                ImageLabel label = ImageLabel.builder()
                    .imageId(labelRequest.imageId())
                    .frameState(labelRequest.frameState())
                    .visibility(labelRequest.visibility())
                    .labelSource(LABEL_SOURCE_ADMIN)
                    .updatedBy(request.updatedBy() != null ? request.updatedBy() : "admin")
                    .updatedAt(isoNow())
                    .build();

                labels.add(label);
            } catch (Exception e) {
                String error = "Invalid label for imageId " + labelRequest.imageId() + ": " + e.getMessage();
                errors.add(error);
                log.warn(error);
            }
        }

        if (!errors.isEmpty()) {
            return BatchLabelResponse.error("Validation errors", errors);
        }

        // Save all labels in batch
        try {
            labelRepository.saveAll(labels);
            log.info("Successfully saved {} admin labels", labels.size());
            return BatchLabelResponse.success(labels.size());
        } catch (Exception e) {
            log.error("Failed to save batch labels: {}", e.getMessage(), e);
            return BatchLabelResponse.error("Failed to save labels");
        }
    }

    /**
     * Retrieves all labels within a date range by scanning imageId prefixes.
     */
    private List<ImageLabel> getLabelsInDateRange(LocalDate startDate, LocalDate endDate) {
        List<ImageLabel> allLabels = new ArrayList<>();
        
        // For each date in range, get labels with imageId prefix matching that date
        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            String datePrefix = current.format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
            
            // Since we don't have a direct date range query, we'll need to scan
            // This is not ideal for large datasets, but acceptable for admin use
            // TODO: Consider adding a date-based GSI if this becomes a performance issue
            List<ImageLabel> dayLabels = getLabelsForDatePrefix(datePrefix);
            allLabels.addAll(dayLabels);
            
            current = current.plusDays(1);
        }
        
        return allLabels;
    }

    /**
     * Gets labels for a specific date prefix (YYYY/MM/DD).
     * Uses the new findByImageIdPrefix method for better performance.
     */
    private List<ImageLabel> getLabelsForDatePrefix(String datePrefix) {
        return labelRepository.findByImageIdPrefix(datePrefix, 1000);
    }

    private LabelDto toLabelDto(ImageLabel label) {
        return new LabelDto(
            label.getImageId(),
            label.getFrameState(),
            label.getVisibility(),
            label.getLabelSource(),
            label.getUpdatedBy(),
            label.getUpdatedAt()
        );
    }

    private String isoNow() {
        return java.time.Instant.now()
                .atOffset(java.time.ZoneOffset.UTC)
                .withNano(0)
                .toString()
                .replace("+00:00", "Z");
    }
}
