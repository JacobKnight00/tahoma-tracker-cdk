package com.tahomatracker.service.modules;

import javax.inject.Singleton;

import com.tahomatracker.service.api.LabelApiConfig;
import com.tahomatracker.service.external.DynamoDbLabelRepository;
import com.tahomatracker.service.external.LabelRepository;
import com.tahomatracker.service.external.ObjectStorageClient;
import com.tahomatracker.service.external.S3ObjectStorageClient;

import dagger.Module;
import dagger.Provides;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Dagger module providing Label API specific dependencies.
 *
 * This module provides the config and wires up repositories and storage clients
 * with the configuration values specific to the Label API Lambda.
 */
@Module
public class LabelApiModule {

    private final LabelApiConfig config;

    public LabelApiModule(LabelApiConfig config) {
        this.config = config;
    }

    @Provides
    @Singleton
    LabelApiConfig provideConfig() {
        return config;
    }

    @Provides
    @Singleton
    LabelRepository provideLabelRepository(DynamoDbEnhancedClient enhancedClient, LabelApiConfig config) {
        return new DynamoDbLabelRepository(enhancedClient, config.imageLabelsTableName());
    }

    @Provides
    @Singleton
    ObjectStorageClient provideObjectStorageClient(S3Client s3Client, LabelApiConfig config) {
        return new S3ObjectStorageClient(s3Client, config.bucketName());
    }
}
