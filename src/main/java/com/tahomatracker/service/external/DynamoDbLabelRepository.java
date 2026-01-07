package com.tahomatracker.service.external;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.extern.slf4j.Slf4j;
import com.tahomatracker.service.domain.ImageLabel;

import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbIndex;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.BatchWriteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.WriteBatch;

/**
 * DynamoDB implementation of LabelRepository using the Enhanced Client.
 */
@Slf4j
public class DynamoDbLabelRepository implements LabelRepository {

    private static final String FRAME_STATE_GSI = "ByFrameState";
    private static final String VISIBILITY_GSI = "ByVisibility";
    private static final int BATCH_SIZE = 25; // DynamoDB batch write limit

    private final DynamoDbEnhancedClient enhancedClient;
    private final DynamoDbTable<ImageLabel> table;
    private final DynamoDbIndex<ImageLabel> frameStateIndex;
    private final DynamoDbIndex<ImageLabel> visibilityIndex;

    public DynamoDbLabelRepository(DynamoDbEnhancedClient enhancedClient, String tableName) {
        this.enhancedClient = enhancedClient;
        this.table = enhancedClient.table(tableName, TableSchema.fromBean(ImageLabel.class));
        this.frameStateIndex = table.index(FRAME_STATE_GSI);
        this.visibilityIndex = table.index(VISIBILITY_GSI);
    }

    @Override
    public void save(ImageLabel label) {
        table.putItem(label);
    }

    @Override
    public void saveAll(List<ImageLabel> labels) {
        if (labels == null || labels.isEmpty()) {
            return;
        }

        // Process in batches of 25 (DynamoDB limit)
        for (int i = 0; i < labels.size(); i += BATCH_SIZE) {
            int end = Math.min(i + BATCH_SIZE, labels.size());
            List<ImageLabel> batch = labels.subList(i, end);

            WriteBatch.Builder<ImageLabel> writeBatchBuilder = WriteBatch.builder(ImageLabel.class)
                .mappedTableResource(table);

            for (ImageLabel label : batch) {
                writeBatchBuilder.addPutItem(label);
            }

            BatchWriteItemEnhancedRequest batchRequest = BatchWriteItemEnhancedRequest.builder()
                .writeBatches(writeBatchBuilder.build())
                .build();

            enhancedClient.batchWriteItem(batchRequest);
        }
    }

    @Override
    public Optional<ImageLabel> findById(String imageId) {
        Key key = Key.builder().partitionValue(imageId).build();
        ImageLabel label = table.getItem(key);
        return Optional.ofNullable(label);
    }

    @Override
    public List<ImageLabel> findByFrameState(String frameState, int limit) {
        if (frameState == null) {
            return Collections.emptyList();
        }
        
        QueryConditional queryConditional = QueryConditional.keyEqualTo(
            Key.builder().partitionValue(frameState).build()
        );

        List<ImageLabel> results = new ArrayList<>();

        frameStateIndex.query(r -> r.queryConditional(queryConditional).limit(limit))
            .stream()
            .flatMap(page -> page.items().stream())
            .limit(limit)
            .forEach(results::add);

        return results;
    }

    @Override
    public List<ImageLabel> findByVisibility(String visibility, int limit) {
        if (visibility == null) {
            return Collections.emptyList(); // No items in GSI have null visibility
        }
        
        QueryConditional queryConditional = QueryConditional.keyEqualTo(
            Key.builder().partitionValue(visibility).build()
        );

        List<ImageLabel> results = new ArrayList<>();

        visibilityIndex.query(r -> r.queryConditional(queryConditional).limit(limit))
            .stream()
            .flatMap(page -> page.items().stream())
            .limit(limit)
            .forEach(results::add);

        return results;
    }

    @Override
    public long countByFrameState(String frameState) {
        if (frameState == null) {
            return 0;
        }
        
        QueryConditional queryConditional = QueryConditional.keyEqualTo(
            Key.builder().partitionValue(frameState).build()
        );

        // Count by iterating through pages
        long count = frameStateIndex.query(r -> r.queryConditional(queryConditional))
            .stream()
            .mapToLong(page -> page.items().size())
            .sum();

        return count;
    }

    @Override
    public long countByVisibility(String visibility) {
        if (visibility == null) {
            return 0;
        }
        
        QueryConditional queryConditional = QueryConditional.keyEqualTo(
            Key.builder().partitionValue(visibility).build()
        );

        // Count by iterating through pages
        long count = visibilityIndex.query(r -> r.queryConditional(queryConditional))
            .stream()
            .mapToLong(page -> page.items().size())
            .sum();

        return count;
    }
}
