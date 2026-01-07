package com.tahomatracker.service.domain;

import com.tahomatracker.service.enums.FrameState;
import com.tahomatracker.service.enums.Visibility;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;

/**
 * Represents a human-provided label for an image.
 *
 * This entity is stored in DynamoDB and used for:
 * - Training ML models (export by classification)
 * - Admin labeling workflow (find unlabeled/pending images)
 * - Crowdsourced label collection (vote tracking)
 *
 * The primary key is the imageId (e.g., "2024/12/23/0400").
 * A GSI on classification enables efficient queries by label type.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@DynamoDbBean
public class ImageLabel {

    /**
     * Primary key: image identifier in format "YYYY/MM/DD/HHmm".
     * Example: "2024/12/23/0400"
     * Use getImageIdObject().toIsoTimestamp() to get ISO 8601 timestamp.
     */
    private String imageId;

    /**
     * Frame state classification: "good", "off_target", "dark", "bad".
     * Stored as string for DynamoDB compatibility.
     */
    private String frameState;

    /**
     * Visibility classification: "out", "partially_out", "not_out".
     * Only set if frameState is "good". Stored as string.
     * Null for non-good frames.
     */
    private String visibility;

    /**
     * Source of the label: "admin", "crowd", "admin_override".
     * - "admin": Human admin directly labeled this
     * - "crowd": Derived from crowdsourced votes (no admin label)
     * - "admin_override": Admin changed a previously crowd-labeled image
     */
    private String labelSource;

    /**
     * For admin labels: admin email or username
     * For crowd labels: "crowd_consensus" or voter ID
     * Optional: can be null for migrated data if not tracked.
     * ISO 8601 timestamp of when the label was last updated.
     */
    private String updatedAt;

    /**
     * Identifier of who last updated the label.
     */
    private String updatedBy;

    /* Optional: only populated when crowd voting is active.
     * Admin labels can coexist with voteCounts (admin overrides majority).
     */
    private VoteCounts voteCounts;

    /**
     * Flag indicating if this label needs review due to voting disagreement.
     * Set to true when:
     * - Crowd votes have no clear majority (e.g., 40% vs 35% vs 25%)
     * - Admin label conflicts with strong crowd consensus
     * Optional: defaults to false/null

    /**
     * Flag indicating if this label needs review due to voting disagreement.
     */
    private Boolean needsReview;

    @DynamoDbPartitionKey
    @DynamoDbSecondarySortKey(indexNames = {"ByFrameState", "ByVisibility"})
    public String getImageId() {
        return imageId;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = "ByFrameState")
    public String getFrameState() {
        return frameState;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = "ByVisibility")
    public String getVisibility() {
        return visibility;
    }

    /**
     * Returns the frame state as an enum.
     */
    public FrameState getFrameStateEnum() {
        return FrameState.fromValue(frameState);
    }

    /**
     * Sets the frame state from an enum.
     */
    public void setFrameStateEnum(FrameState state) {
        this.frameState = state != null ? state.getValue() : null;
    }

    /**
     * Returns the visibility as an enum.
     */
    public Visibility getVisibilityEnum() {
        return Visibility.fromValue(visibility);
    }

    /**
     * Sets the visibility from an enum.
     * Validates that visibility can only be set when frameState is "good".
     */
    public void setVisibilityEnum(Visibility vis) {
        setVisibility(vis != null ? vis.getValue() : null);
    }

    /**
     * Sets the visibility with validation.
     * Visibility can only be set when frameState is "good".
     * 
     * @throws IllegalArgumentException if visibility is set on a non-good frame
     */
    public void setVisibility(String visibility) {
        if (visibility != null && !"good".equals(this.frameState)) {
            throw new IllegalArgumentException(
                "Visibility can only be set when frameState is 'good', not: " + this.frameState
            );
        }
        this.visibility = visibility;
    }

    /**
     * Converts image ID to ISO timestamp.
     * Example: "2024/12/23/0400" -> "2024-12-23T04:00:00Z"
     */
    public static String imageIdToTimestamp(String imageId) {
        if (imageId == null || imageId.length() < 15) {
            return null;
        }
        try {
            return ImageId.of(imageId).toIsoTimestamp();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Converts ISO timestamp to image ID.
     * Example: "2024-12-23T04:00:00Z" -> "2024/12/23/0400"
     */
    public static String timestampToImageId(String timestamp) {
        if (timestamp == null || timestamp.length() < 16) {
            return null;
        }
        try {
            return ImageId.fromIsoTimestamp(timestamp).getValue();
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    /**
     * Returns the imageId as a typed ImageId value object.
     * Throws IllegalArgumentException if imageId is not in valid format.
     */
    public ImageId getImageIdObject() {
        return imageId != null ? ImageId.parse(imageId) : null;
    }

    /**
     * Sets the imageId from a typed ImageId value object.
     */
    public void setImageIdObject(ImageId id) {
        this.imageId = id != null ? id.getValue() : null;
    }
}
