package com.tahomatracker.di;

import javax.inject.Singleton;

import dagger.Module;
import dagger.Provides;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * Dagger module providing shared AWS SDK clients.
 *
 * Clients are singletons to enable connection reuse across Lambda invocations,
 * reducing cold start latency. This module is shared across all Lambda handlers.
 */
@Module
public class AwsClientsModule {

    @Provides
    @Singleton
    S3Client provideS3Client() {
        return S3Client.builder().build();
    }

    @Provides
    @Singleton
    DynamoDbClient provideDynamoDbClient() {
        return DynamoDbClient.builder().build();
    }

    @Provides
    @Singleton
    DynamoDbEnhancedClient provideDynamoDbEnhancedClient(DynamoDbClient dynamoDbClient) {
        return DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
    }
}
