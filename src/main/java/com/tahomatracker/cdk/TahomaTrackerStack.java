package com.tahomatracker.cdk;

import java.util.List;
import java.util.Map;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.GlobalSecondaryIndexProps;
import software.amazon.awscdk.services.dynamodb.ProjectionType;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.events.EventField;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleTargetInput;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.CronOptions;
import software.amazon.awscdk.services.events.targets.LambdaFunction;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.FunctionUrl;
import software.amazon.awscdk.services.lambda.FunctionUrlAuthType;
import software.amazon.awscdk.services.lambda.FunctionUrlCorsOptions;
import software.amazon.awscdk.services.lambda.FunctionUrlOptions;
import software.amazon.awscdk.services.lambda.HttpMethod;
import software.amazon.awscdk.services.lambda.Architecture;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.CfnOutput;
import software.amazon.awscdk.services.cloudfront.BehaviorOptions;
import software.amazon.awscdk.services.cloudfront.CachePolicy;
import software.amazon.awscdk.services.cloudfront.Distribution;
import software.amazon.awscdk.services.cloudfront.OriginAccessIdentity;
import software.amazon.awscdk.services.cloudfront.ViewerProtocolPolicy;
import software.amazon.awscdk.services.cloudfront.IOrigin;
import software.amazon.awscdk.services.cloudfront.origins.S3BucketOrigin;
import software.amazon.awscdk.services.cloudfront.origins.S3BucketOriginWithOAIProps;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.s3.CorsRule;
import software.amazon.awscdk.services.s3.HttpMethods;
import software.constructs.Construct;

public class TahomaTrackerStack extends Stack {
    
    private static final String HEARTBEAT_IMAGE_ID = "9999/12/31/2359";
    
    public TahomaTrackerStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public TahomaTrackerStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        Bucket bucket = Bucket.Builder.create(this, "ArtifactsBucket")
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(RemovalPolicy.RETAIN)
                .cors(java.util.List.of(CorsRule.builder()
                        .allowedMethods(java.util.List.of(HttpMethods.GET, HttpMethods.HEAD))
                        .allowedOrigins(java.util.List.of("*"))
                        .allowedHeaders(java.util.List.of("*"))
                        .maxAge(3600)
                        .build()))
                .build();

        // Image labels table for storing human-provided ground truth labels
        // Used for ML training, admin labeling tool, and crowdsourcing
        Table labelsTable = Table.Builder.create(this, "ImageLabelsTable")
                .tableName("TahomaTrackerImageLabels")
                .partitionKey(Attribute.builder()
                        .name("imageId")
                        .type(AttributeType.STRING)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)  // On-demand pricing
                .removalPolicy(RemovalPolicy.RETAIN)
                .build();
        
        // GSI for querying by frame state (good, dark, bad, off_target)
        // Enables efficient queries for training data by frame quality
        labelsTable.addGlobalSecondaryIndex(GlobalSecondaryIndexProps.builder()
                .indexName("ByFrameState")
                .partitionKey(Attribute.builder()
                        .name("frameState")
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name("imageId")
                        .type(AttributeType.STRING)
                        .build())
                .projectionType(ProjectionType.ALL)
                .build());

        // GSI for querying by visibility (out, partially_out, not_out)
        // This is a SPARSE index - only "good" frames have visibility values
        // Enables efficient queries for visibility classification training
        labelsTable.addGlobalSecondaryIndex(GlobalSecondaryIndexProps.builder()
                .indexName("ByVisibility")
                .partitionKey(Attribute.builder()
                        .name("visibility")
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name("imageId")
                        .type(AttributeType.STRING)
                        .build())
                .projectionType(ProjectionType.ALL)
                .build());

        Function pipelineFn = Function.Builder.create(this, "PipelineFunction")
                        .runtime(Runtime.JAVA_21)
                        .architecture(Architecture.X86_64)
                        .handler("com.tahomatracker.service.ScraperHandler::handleRequest")
                        .code(Code.fromAsset("target/tahomacdk-0.1.jar"))
                        .memorySize(1024)
                        .timeout(Duration.minutes(5))  // Increased for automatic backfill
                        .environment(Map.ofEntries(
                                Map.entry("BUCKET_NAME", bucket.getBucketName()),
                                Map.entry("IMAGE_LABELS_TABLE_NAME", labelsTable.getTableName()),
                                Map.entry("CAMERA_BASE_URL", "https://d3omclagh7m7mg.cloudfront.net/assets"),
                                Map.entry("PANOS_PREFIX", "needle-cam/panos"),
                                Map.entry("CROPPED_PREFIX", "needle-cam/cropped-images"),
                                Map.entry("ANALYSIS_PREFIX", "analysis"),
                                Map.entry("MODELS_PREFIX", "models"),
                                Map.entry("CROP_BOX", "3975,200,4575,650"),
                                Map.entry("OUT_THRESHOLD", "0.85"),
                                Map.entry("MODEL_VERSION", "v1"),
                                Map.entry("LOCAL_TZ", "America/Los_Angeles"),
                                Map.entry("WINDOW_START_HOUR", "4"),
                                Map.entry("WINDOW_END_HOUR", "23"),
                                Map.entry("STEP_MINUTES", "10"),
                                Map.entry("BACKFILL_LOOKBACK_HOURS", "6"),
                                Map.entry("MANIFESTS_PREFIX", "manifests")
                        ))
                        .build();

        labelsTable.grantReadWriteData(pipelineFn);
        bucket.grantReadWrite(pipelineFn);

        // Label API Lambda - handles crowdsource label submissions
        // Uses Function URL for direct HTTP access (no API Gateway costs)
        List<String> labelApiAllowedOrigins = List.of(
                "https://tahomatracker.com",
                "https://tahoma-tracker-site.pages.dev",
                "http://localhost:8080",
                "http://localhost:8081",
                "http://localhost:8082",
                "http://localhost:8083",
                "http://localhost:8084",
                "http://localhost:8085",
                "http://localhost:8086",
                "http://localhost:8087",
                "http://localhost:8088",
                "http://localhost:8089",
                "http://localhost:8090"
        );

        Function labelApiFn = Function.Builder.create(this, "LabelApiFunction")
                .runtime(Runtime.JAVA_21)
                .architecture(Architecture.X86_64)
                .handler("com.tahomatracker.service.api.LabelApiHandler::handleRequest")
                .code(Code.fromAsset("target/tahomacdk-0.1.jar"))
                .memorySize(512)
                .timeout(Duration.seconds(30))
                .environment(Map.ofEntries(
                        Map.entry("BUCKET_NAME", bucket.getBucketName()),
                        Map.entry("IMAGE_LABELS_TABLE_NAME", labelsTable.getTableName()),
                        Map.entry("ANALYSIS_PREFIX", "analysis"),
                        Map.entry("MODEL_VERSION", "v1"),
                        Map.entry("ALLOWED_ORIGINS", String.join(",", labelApiAllowedOrigins))
                ))
                .build();

        labelsTable.grantReadWriteData(labelApiFn);
        bucket.grantRead(labelApiFn);

        // Function URL with CORS for public access
        FunctionUrl labelApiUrl = labelApiFn.addFunctionUrl(FunctionUrlOptions.builder()
                .authType(FunctionUrlAuthType.NONE)
                .cors(FunctionUrlCorsOptions.builder()
                        .allowedOrigins(labelApiAllowedOrigins)
                        .allowedMethods(java.util.List.of(HttpMethod.POST))
                        .allowedHeaders(java.util.List.of("content-type", "authorization"))
                        .maxAge(Duration.hours(1))
                        .build())
                .build());

        // Admin API Lambda - handles admin label operations
        // Uses Function URL for direct HTTP access (no API Gateway costs)
        Function adminApiFn = Function.Builder.create(this, "AdminApiFunction")
                .runtime(Runtime.JAVA_21)
                .architecture(Architecture.X86_64)
                .handler("com.tahomatracker.service.api.AdminApiHandler::handleRequest")
                .code(Code.fromAsset("target/tahomacdk-0.1.jar"))
                .memorySize(512)
                .timeout(Duration.seconds(30))
                .environment(Map.ofEntries(
                        Map.entry("BUCKET_NAME", bucket.getBucketName()),
                        Map.entry("IMAGE_LABELS_TABLE_NAME", labelsTable.getTableName()),
                        Map.entry("ANALYSIS_PREFIX", "analysis"),
                        Map.entry("MODEL_VERSION", "v1"),
                        Map.entry("ALLOWED_ORIGINS", String.join(",", labelApiAllowedOrigins))
                ))
                .build();

        labelsTable.grantReadWriteData(adminApiFn);
        bucket.grantRead(adminApiFn);

        // Function URL with CORS for admin access
        FunctionUrl adminApiUrl = adminApiFn.addFunctionUrl(FunctionUrlOptions.builder()
                .authType(FunctionUrlAuthType.NONE)
                .cors(FunctionUrlCorsOptions.builder()
                        .allowedOrigins(labelApiAllowedOrigins)
                        .allowedMethods(java.util.List.of(HttpMethod.GET, HttpMethod.POST))
                        .allowedHeaders(java.util.List.of("content-type", "authorization"))
                        .maxAge(Duration.hours(1))
                        .build())
                .build());

        // Keep Label API function warm with a periodic POST to test endpoint
        Rule.Builder.create(this, "LabelApiWarmupSchedule")
                .description("Keeps Label API Lambda warm with a test POST request every 5 minutes")
                .schedule(Schedule.rate(Duration.minutes(5)))
                .targets(java.util.List.of(
                        new LambdaFunction(labelApiFn,
                                software.amazon.awscdk.services.events.targets.LambdaFunctionProps.builder()
                                        .event(RuleTargetInput.fromObject(Map.of(
                                                "requestContext", Map.of("http", Map.of("method", "POST")),
                                                "body", "{\"imageId\":\"" + HEARTBEAT_IMAGE_ID + "\",\"frameState\":\"good\",\"visibility\":\"out\"}",
                                                "isBase64Encoded", false
                                        )))
                                        .build())
                ))
                .build();

        // EventBridge Rule: Trigger Lambda every 2 minutes with automatic backfill
        Rule.Builder.create(this, "ProcessingSchedule")
                .description("Triggers Lambda every 2 minutes for image processing with automatic backfill")
                .schedule(Schedule.cron(CronOptions.builder()
                        .minute("0/2")  // Fire every 2 minutes
                        .build()))
                .targets(java.util.List.of(
                        new LambdaFunction(pipelineFn, software.amazon.awscdk.services.events.targets.LambdaFunctionProps.builder()
                                .event(RuleTargetInput.fromObject(Map.of(
                                        "timestamp", EventField.fromPath("$.time")
                                )))
                                .build())))
                .build();

        // CloudFront Distribution for public access to S3 content
        // Create Origin Access Identity to allow CloudFront to access private S3 bucket
        OriginAccessIdentity oai = OriginAccessIdentity.Builder.create(this, "OAI")
                .comment("Origin Access Identity for Tahoma Tracker")
                .build();

        // Grant CloudFront OAI read access to the S3 bucket
        bucket.grantRead(oai);

        IOrigin origin = S3BucketOrigin.withOriginAccessIdentity(bucket,
                S3BucketOriginWithOAIProps.builder()
                        .originAccessIdentity(oai)
                        .build());

        CachePolicy latestCachePolicy = CachePolicy.Builder.create(this, "LatestCachePolicy")
                .minTtl(Duration.seconds(30))
                .defaultTtl(Duration.seconds(30))
                .maxTtl(Duration.seconds(30))
                .build();

        // Create CloudFront distribution
        Distribution distribution = Distribution.Builder.create(this, "Distribution")
                .comment("Tahoma Tracker CDN")
                .defaultBehavior(BehaviorOptions.builder()
                        .origin(origin)
                        .viewerProtocolPolicy(ViewerProtocolPolicy.REDIRECT_TO_HTTPS)
                        .cachePolicy(CachePolicy.CACHING_OPTIMIZED)
                        .build())
                .additionalBehaviors(Map.of(
                        "latest/*", BehaviorOptions.builder()
                                .origin(origin)
                                .viewerProtocolPolicy(ViewerProtocolPolicy.REDIRECT_TO_HTTPS)
                                .cachePolicy(latestCachePolicy)
                                .build(),
                        "manifests/daily/current.json", BehaviorOptions.builder()
                                .origin(origin)
                                .viewerProtocolPolicy(ViewerProtocolPolicy.REDIRECT_TO_HTTPS)
                                .cachePolicy(latestCachePolicy)
                                .build(),
                        "manifests/monthly/current.json", BehaviorOptions.builder()
                                .origin(origin)
                                .viewerProtocolPolicy(ViewerProtocolPolicy.REDIRECT_TO_HTTPS)
                                .cachePolicy(latestCachePolicy)
                                .build()
                ))
                .build();

        // Output the CloudFront distribution URL
        CfnOutput.Builder.create(this, "DistributionUrl")
                .value("https://" + distribution.getDistributionDomainName())
                .description("CloudFront distribution URL for accessing S3 content")
                .exportName("TahomaTrackerDistributionUrl")
                .build();

        CfnOutput.Builder.create(this, "DistributionDomain")
                .value(distribution.getDistributionDomainName())
                .description("CloudFront distribution domain name")
                .exportName("TahomaTrackerDistributionDomain")
                .build();

        CfnOutput.Builder.create(this, "BucketName")
                .value(bucket.getBucketName())
                .description("S3 bucket name")
                .exportName("TahomaTrackerBucketName")
                .build();

        CfnOutput.Builder.create(this, "LabelApiUrl")
                .value(labelApiUrl.getUrl())
                .description("Label API Function URL for crowdsource label submissions")
                .exportName("TahomaTrackerLabelApiUrl")
                .build();

        CfnOutput.Builder.create(this, "AdminApiUrl")
                .value(adminApiUrl.getUrl())
                .description("Admin API Function URL for admin label operations")
                .exportName("TahomaTrackerAdminApiUrl")
                .build();
    }
}
