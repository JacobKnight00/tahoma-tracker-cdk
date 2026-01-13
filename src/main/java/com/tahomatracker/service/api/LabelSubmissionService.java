package com.tahomatracker.service.api;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.tahomatracker.service.api.dto.LabelRequest;
import com.tahomatracker.service.api.dto.LabelResponse;
import com.tahomatracker.service.domain.ImageLabel;
import com.tahomatracker.service.domain.VoteCounts;
import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;
import com.tahomatracker.service.external.LabelRepository;
import com.tahomatracker.service.external.ObjectStorageClient;

import lombok.extern.slf4j.Slf4j;

/**
 * Service handling crowdsource label submissions.
 *
 * Vote counting logic:
 * - Each submission increments vote counts for the submitted frameState/visibility
 * - If no admin label exists, the leading vote becomes the effective label
 * - If admin label exists, votes are recorded but don't override admin decision
 * - Visibility is only set if the leading frameState is "good"
 */
@Slf4j
@Singleton
public class LabelSubmissionService {

    private static final String LABEL_SOURCE_CROWD = "crowd";
    private static final String LABEL_SOURCE_ADMIN = "admin";

    private final LabelRepository labelRepository;
    private final ObjectStorageClient storageClient;
    private final LabelApiConfig config;

    @Inject
    public LabelSubmissionService(
            LabelRepository labelRepository,
            ObjectStorageClient storageClient,
            LabelApiConfig config) {
        this.labelRepository = labelRepository;
        this.storageClient = storageClient;
        this.config = config;
    }

    /**
     * Submits a crowdsource label vote.
     */
    public LabelResponse submitLabel(LabelRequest request) {
        Optional<LabelResponse> validationError = validateRequest(request);
        if (validationError.isPresent()) {
            return validationError.get();
        }

        Optional<LabelResponse> imageError = validateImageExists(request.imageId());
        if (imageError.isPresent()) {
            return imageError.get();
        }

        ImageLabel label = getOrCreateLabel(request.imageId());
        processVote(label, request);
        return saveLabel(label, request);
    }

    private Optional<LabelResponse> validateRequest(LabelRequest request) {
        String error = request.validate();
        if (error != null) {
            log.info("Validation failed for imageId={}: {}", request.imageId(), error);
            return Optional.of(LabelResponse.error(error));
        }
        return Optional.empty();
    }

    private Optional<LabelResponse> validateImageExists(String imageId) {
        String analysisKey = config.analysisKey(imageId);
        try {
            if (!storageClient.exists(analysisKey)) {
                log.info("Image not found: {}", imageId);
                return Optional.of(LabelResponse.error("Image not found: " + imageId));
            }
        } catch (IOException e) {
            log.error("Failed to check image existence: {}", e.getMessage(), e);
            return Optional.of(LabelResponse.error("Failed to verify image exists"));
        }
        return Optional.empty();
    }

    private ImageLabel getOrCreateLabel(String imageId) {
        return labelRepository.findById(imageId)
                .orElseGet(() -> ImageLabel.builder()
                        .imageId(imageId)
                        .labelSource(LABEL_SOURCE_CROWD)
                        .voteCounts(new VoteCounts())
                        .build());
    }

    private void processVote(ImageLabel label, LabelRequest request) {
        recordVote(label, request.frameStateEnum(), request.visibilityEnum());

        if (!isAdminLabeled(label)) {
            updateDerivedLabel(label);
        } else {
            log.debug("Preserving admin label for {}", label.getImageId());
        }

        label.setUpdatedAt(isoNow());
        label.setUpdatedBy("crowdsource");
    }

    private LabelResponse saveLabel(ImageLabel label, LabelRequest request) {
        try {
            labelRepository.save(label);
            log.info("Vote recorded: imageId={}, frameState={}, visibility={}",
                    request.imageId(), request.frameState(), request.visibility());
            return LabelResponse.success("Label recorded");
        } catch (Exception e) {
            log.error("Failed to save label: {}", e.getMessage(), e);
            return LabelResponse.error("Failed to save label");
        }
    }

    private void recordVote(ImageLabel label, FrameState frameState, Visibility visibility) {
        VoteCounts counts = label.getVoteCounts();
        if (counts == null) {
            counts = new VoteCounts();
            label.setVoteCounts(counts);
        }

        counts.addFrameStateVote(frameState);
        if (visibility != null) {
            counts.addVisibilityVote(visibility);
        }
    }

    private boolean isAdminLabeled(ImageLabel label) {
        String source = label.getLabelSource();
        return LABEL_SOURCE_ADMIN.equals(source) || "admin_override".equals(source);
    }

    private void updateDerivedLabel(ImageLabel label) {
        VoteCounts counts = label.getVoteCounts();
        if (counts == null) {
            return;
        }

        FrameState leadingFrameState = counts.getMostCommonFrameState();
        if (leadingFrameState == null) {
            return;
        }

        label.setFrameState(leadingFrameState.getValue());
        label.setLabelSource(LABEL_SOURCE_CROWD);

        if (leadingFrameState == FrameState.GOOD) {
            Visibility leadingVisibility = counts.getMostCommonVisibility();
            label.setVisibility(leadingVisibility != null ? leadingVisibility.getValue() : null);
        } else {
            label.setVisibility(null);
        }
    }

    private String isoNow() {
        return Instant.now()
                .atOffset(ZoneOffset.UTC)
                .withNano(0)
                .toString()
                .replace("+00:00", "Z");
    }
}
