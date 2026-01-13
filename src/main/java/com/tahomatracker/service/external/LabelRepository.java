package com.tahomatracker.service.external;

import java.util.List;
import java.util.Optional;

import com.tahomatracker.service.domain.ImageLabel;

/**
 * Repository interface for image label storage and retrieval.
 *
 * This table uses a SPARSE design: only images with actual labels (admin or crowd)
 * have entries. Unlabeled images exist only in S3, not in this table.
 *
 * To find images to label, query S3 for available images and check if they exist
 * in this table. If not present = needs labeling.
 *
 * Abstracts the underlying storage mechanism (DynamoDB) to enable
 * testing with mock implementations.
 */
public interface LabelRepository {

    /**
     * Saves a label.
     */
    void save(ImageLabel label);

    /**
     * Saves multiple labels in a batch operation.
     */
    void saveAll(List<ImageLabel> labels);

    /**
     * Retrieves a label by image ID.
     *
     * @param imageId the image identifier (e.g., "2024/12/23/0400")
     * @return the label if found, empty otherwise
     */
    Optional<ImageLabel> findById(String imageId);

    /**
     * Retrieves labels by frame state.
     *
     * @param frameState the frame state value (e.g., "good", "dark", "bad", "off_target")
     * @param limit maximum number of results to return
     * @return list of matching labels
     */
    List<ImageLabel> findByFrameState(String frameState, int limit);

    /**
     * Retrieves labels by visibility.
     * Note: Only returns items with non-null visibility (i.e., "good" frames).
     *
     * @param visibility the visibility value (e.g., "out", "partially_out", "not_out")
     * @param limit maximum number of results to return
     * @return list of matching labels
     */
    List<ImageLabel> findByVisibility(String visibility, int limit);

    /**
     * Counts labels with the given frame state.
     *
     * @param frameState the frame state value
     * @return count of matching labels
     */
    long countByFrameState(String frameState);

    /**
     * Counts labels with the given visibility.
     *
     * @param visibility the visibility value
     * @return count of matching labels
     */
    long countByVisibility(String visibility);

    /**
     * Checks if a label exists for the given image ID.
     */
    default boolean exists(String imageId) {
        return findById(imageId).isPresent();
    }

    /**
     * Retrieves labels with imageId starting with the given prefix.
     * Useful for date-based queries (e.g., prefix "2025/01/10").
     * 
     * @param imageIdPrefix the prefix to match (e.g., "2025/01/10")
     * @param limit maximum number of results to return
     * @return list of matching labels
     */
    List<ImageLabel> findByImageIdPrefix(String imageIdPrefix, int limit);

    /**
     * Retrieves all labels (scan operation).
     * Use with caution - can be expensive for large datasets.
     * 
     * @param limit maximum number of results to return
     * @return list of all labels
     */
    List<ImageLabel> findAll(int limit);
}
