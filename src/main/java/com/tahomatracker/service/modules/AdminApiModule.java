package com.tahomatracker.service.modules;

import javax.inject.Singleton;

import com.tahomatracker.service.api.AdminApiConfig;
import com.tahomatracker.service.api.AdminLabelService;
import com.tahomatracker.service.external.DynamoDbLabelRepository;
import com.tahomatracker.service.external.LabelRepository;

import dagger.Module;
import dagger.Provides;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/**
 * Dagger module providing dependencies for Admin API Lambda.
 */
@Module(includes = AwsClientsModule.class)
public class AdminApiModule {

    private final AdminApiConfig config;

    public AdminApiModule(AdminApiConfig config) {
        this.config = config;
    }

    @Provides
    @Singleton
    public AdminApiConfig provideConfig() {
        return config;
    }

    @Provides
    @Singleton
    public LabelRepository provideLabelRepository(DynamoDbClient dynamoDbClient) {
        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(dynamoDbClient)
                .build();
        return new DynamoDbLabelRepository(enhancedClient, config.imageLabelsTableName());
    }

    @Provides
    @Singleton
    public AdminLabelService provideAdminLabelService(LabelRepository labelRepository) {
        return new AdminLabelService(labelRepository, config);
    }
}
