# GitHub Copilot instructions — Tahoma Tracker (CDK)

Quick, actionable guidance for AI coding agents working on this repository.

## Big picture (what this repo does) 🔧
- This is an AWS CDK (Java) application that declares infrastructure to run a periodic image processing pipeline.
- Key runtime pieces (see `src/main/java/com/tahomatracker/cdk/TahomaTrackerStack.java`):
  - S3 Bucket (`ArtifactsBucket`) for storing panoramas, cropped images and analysis JSON.
  - DynamoDB table (`TahomaTrackerLabels`) with `date` (PK) and `time` (SK). Table is created but not actively used by current code.
  - Lambda (`PipelineFunction`) that runs the scraping & processing logic (handler: `com.tahomatracker.service.ScraperHandler::handleRequest`).
  - Step Functions state machine wrapping the Lambda; an EventBridge rule triggers the state machine every 10 minutes.

## Important workflows & commands ✅
- Build + package the CDK app & Lambda jar: `mvn package` (produces `target/tahomacdk-<version>.jar`).
- CDK synth: `cdk synth`
- CDK deploy: `cdk deploy` (note: building Lambda assets may require Docker as mentioned in `README.md`).
- Run tests: `mvn test` (there are example tests commented out in `src/test`).

## Key files to inspect when changing behavior 📂
- `src/main/java/com/tahomatracker/service/ScraperHandler.java` — core pipeline code (fetching camera slices, stitching, cropping, S3 uploads, JSON analysis payload).
- `src/main/java/com/tahomatracker/cdk/TahomaTrackerStack.java` — how infra, env vars, and resource permissions are declared.
- `pom.xml` — artifact versioning (jar name under `target/`), Java release config, and the `maven-shade-plugin` entry points.

## Configuration & environment variables (explicit defaults shown in code) 🧩
- `BUCKET_NAME` — S3 bucket (set by CDK env for the function at deploy time).
- `LABELS_TABLE_NAME` — DynamoDB table name (`TahomaTrackerLabels`).
- `CAMERA_BASE_URL` (default in code: `https://d3omclagh7m7mg.cloudfront.net/assets`) — base URL for slices.
- `PANOS_PREFIX`, `CROPPED_PREFIX`, `ANALYSIS_PREFIX`, `MODELS_PREFIX` — S3 prefixes used to store artifacts.
- `LATEST_KEY` (default `latest/latest.json`) — where the "latest" JSON is written.
- `CROP_BOX` — comma-separated `x1,y1,x2,y2` bounding box; if blank, full pano is used.
- `OUT_THRESHOLD`, `MODEL_VERSION` — numeric and model metadata used in analysis JSON.
- `LOCAL_TZ`, `WINDOW_START_HOUR`, `WINDOW_END_HOUR`, `STEP_MINUTES` — control the time window and step granularity used by the pipeline.

When adding or renaming env vars, update both `TahomaTrackerStack.java` and `ScraperHandler.java` (search for the variable name in both files).

## Behavior notes & conventions (helpful for code edits) 🔍
- Time handling: the function expects an input `event` with a `ts` string (ISO8601); otherwise it uses current time (UTC). The stack passes a `ts` using EventBridge fields.
- Slicing logic: fetches up to 500 slices; a missing-streak of 3 ends the fetch loop; sleeps 120ms between requests.
- Stitching: images are stitched horizontally, then cropped using `CROP_BOX` and uploaded as JPEGs.
- JSON payloads: written with Jackson (`ObjectMapper`) and uploaded to S3 as `application/json`.
- Existence checks: `analysisExists` uses S3 `headObject` to determine whether analysis JSON already exists.
- Defaults are chosen to be conservative (e.g., removal policy = RETAIN for bucket and table).

## Testing & local development tips 🧪
- There is no local Lambda emulator included. For integration tests, either:
  - run against a real AWS account (set AWS creds & region) and a test bucket/table, or
  - use LocalStack to emulate S3 and Step Functions.
- Unit tests: add focused pure-Java unit tests for `ScraperHandler` helper methods (e.g., `parseCropBox`, `stitchHorizontal`, `formatKey`) without invoking AWS SDK.
- For integration-style tests that hit S3, prefer dedicated test buckets and a short-lived test deploy to avoid polluting prod data.

## Deployment & infra caution ⚠️
- Changing the S3 prefixes, `LATEST_KEY`, or table names must be coordinated between stack and service code.
- `Bucket` and `Table` use `RemovalPolicy.RETAIN` — deleting the stack will not delete stored data.
- Lambda runtime uses `Runtime.JAVA_17` while `pom.xml` sets the maven compiler release; review runtime compatibility when upgrading Java features.

## How to modify safely (recommended steps) 🛠️
1. Update code in `ScraperHandler` and unit tests for logic changes.
2. Update `TahomaTrackerStack` if you add/remove env vars or resources.
3. Run `mvn package` and `cdk synth` locally to validate artifact names and synthesized template.
4. Run `cdk deploy` (use a test account/region for non-backwards compatible changes).

## Quick examples to reference in PRs
- Change a config: add `NEW_FOO` env var -> add entry to `PipelineFunction.environment(...)` in `TahomaTrackerStack.java` and a `private final String newFoo = env("NEW_FOO", "fallback");` in `ScraperHandler.java`.
- Bump artifact version: `pom.xml` version -> update the jar path used in `Code.fromAsset("target/tahomacdk-<version>.jar")` or make it derived dynamically.

---
If anything is unclear or you want more detail (e.g., example unit tests for `ScraperHandler`, localstack test harness, or a checklist for safe infra changes), tell me which section to expand and I'll update this file. ✅
