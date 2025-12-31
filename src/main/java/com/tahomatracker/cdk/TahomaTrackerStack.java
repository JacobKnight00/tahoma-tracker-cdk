package com.tahomatracker.cdk;

import java.util.Map;
import software.amazon.awscdk.Duration;
import software.amazon.awscdk.RemovalPolicy;
import software.amazon.awscdk.Stack;
import software.amazon.awscdk.StackProps;
import software.amazon.awscdk.services.dynamodb.Attribute;
import software.amazon.awscdk.services.dynamodb.AttributeType;
import software.amazon.awscdk.services.dynamodb.BillingMode;
import software.amazon.awscdk.services.dynamodb.Table;
import software.amazon.awscdk.services.events.EventField;
import software.amazon.awscdk.services.events.Rule;
import software.amazon.awscdk.services.events.RuleTargetInput;
import software.amazon.awscdk.services.events.Schedule;
import software.amazon.awscdk.services.events.targets.SfnStateMachine;
import software.amazon.awscdk.services.lambda.Code;
import software.amazon.awscdk.services.lambda.Function;
import software.amazon.awscdk.services.lambda.Runtime;
import software.amazon.awscdk.services.s3.BlockPublicAccess;
import software.amazon.awscdk.services.s3.Bucket;
import software.amazon.awscdk.services.s3.BucketEncryption;
import software.amazon.awscdk.services.stepfunctions.DefinitionBody;
import software.amazon.awscdk.services.stepfunctions.StateMachine;
import software.amazon.awscdk.services.stepfunctions.tasks.LambdaInvoke;
import software.amazon.awscdk.services.stepfunctions.tasks.LambdaInvokeProps;
import software.constructs.Construct;

public class TahomaTrackerStack extends Stack {
    public TahomaTrackerStack(final Construct scope, final String id) {
        this(scope, id, null);
    }

    public TahomaTrackerStack(final Construct scope, final String id, final StackProps props) {
        super(scope, id, props);

        Bucket bucket = Bucket.Builder.create(this, "ArtifactsBucket")
                .blockPublicAccess(BlockPublicAccess.BLOCK_ALL)
                .encryption(BucketEncryption.S3_MANAGED)
                .removalPolicy(RemovalPolicy.RETAIN)
                .build();

        Table labelsTable = Table.Builder.create(this, "LabelsTable")
                .tableName("TahomaTrackerLabels")
                .partitionKey(Attribute.builder()
                        .name("date")
                        .type(AttributeType.STRING)
                        .build())
                .sortKey(Attribute.builder()
                        .name("time")
                        .type(AttributeType.STRING)
                        .build())
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .removalPolicy(RemovalPolicy.RETAIN)
                .build();

        Function pipelineFn = Function.Builder.create(this, "PipelineFunction")
                        .runtime(Runtime.JAVA_17)
                        .handler("com.tahomatracker.service.ScraperHandler::handleRequest")
                        .code(Code.fromAsset("target/tahomacdk-0.1.jar"))
                        .memorySize(1024)
                        .timeout(Duration.minutes(2))
                        .environment(Map.ofEntries(
                                Map.entry("BUCKET_NAME", bucket.getBucketName()),
                                Map.entry("LABELS_TABLE_NAME", labelsTable.getTableName()),
                                Map.entry("CAMERA_BASE_URL", "https://d3omclagh7m7mg.cloudfront.net/assets"),
                                Map.entry("PANOS_PREFIX", "needle-cam/panos"),
                                Map.entry("CROPPED_PREFIX", "needle-cam/cropped-images"),
                                Map.entry("ANALYSIS_PREFIX", "analysis"),
                                Map.entry("LATEST_KEY", "latest/latest.json"),
                                Map.entry("MODELS_PREFIX", "models"),
                                Map.entry("CROP_BOX", "3975,200,4575,650"),
                                Map.entry("OUT_THRESHOLD", "0.85"),
                                Map.entry("MODEL_VERSION", "v1"),
                                Map.entry("LOCAL_TZ", "America/Los_Angeles"),
                                Map.entry("WINDOW_START_HOUR", "4"),
                                Map.entry("WINDOW_END_HOUR", "23"),
                                Map.entry("STEP_MINUTES", "10")
                        ))
                        .build();

        labelsTable.grantReadWriteData(pipelineFn);
        bucket.grantReadWrite(pipelineFn);

        LambdaInvoke pipelineTask = new LambdaInvoke(this, "RunPipeline",
                LambdaInvokeProps.builder()
                        .lambdaFunction(pipelineFn)
                        .payloadResponseOnly(true)
                        .build());

        StateMachine stateMachine = new StateMachine(this, "TahomaTrackerPipeline",
                software.amazon.awscdk.services.stepfunctions.StateMachineProps.builder()
                        .definitionBody(DefinitionBody.fromChainable(pipelineTask))
                        .timeout(Duration.minutes(10))
                        .build());

        pipelineFn.grantInvoke(stateMachine.getRole());

        Rule scheduleRule = Rule.Builder.create(this, "ScheduleEveryTenMinutes")
                .schedule(Schedule.rate(Duration.minutes(10)))
                .targets(java.util.List.of(SfnStateMachine.Builder.create(stateMachine)
                        .input(RuleTargetInput.fromObject(Map.of(
                                "ts", EventField.fromPath("$.time")
                        )))
                        .build()))
                .build();
    }
}
