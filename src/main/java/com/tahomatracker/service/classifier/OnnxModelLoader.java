package com.tahomatracker.service.classifier;

import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtSession;
import ai.onnxruntime.OrtException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

/**
 * Downloads ONNX models from S3 and caches OrtSessions in /tmp.
 */
@Slf4j
public class OnnxModelLoader {
    private final S3Client s3;
    private final String bucket;
    private final Path cacheDir;
    private final OrtEnvironment env;
    private final Map<String, OrtSession> sessions = new ConcurrentHashMap<>();

    public OnnxModelLoader(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
        this.cacheDir = Path.of("/tmp/onnx-cache");
        this.env = OrtEnvironment.getEnvironment();
    }

    public OrtSession loadSession(String modelKey) {
        return sessions.computeIfAbsent(modelKey, this::downloadAndCreateSession);
    }

    private OrtSession downloadAndCreateSession(String modelKey) {
        try {
            Files.createDirectories(cacheDir);
            // Use original filename to preserve relative paths for external data files
            String filename = Path.of(modelKey).getFileName().toString();
            Path localPath = cacheDir.resolve(filename);

            if (!Files.exists(localPath)) {
                log.info("Downloading ONNX model s3://{}/{} to {}", bucket, modelKey, localPath);
                downloadFromS3(modelKey, localPath);
            } else {
                log.info("Reusing cached ONNX model at {}", localPath);
            }

            // ONNX models may use external data files (.onnx.data) that must be co-located
            String dataKey = modelKey + ".data";
            Path dataPath = cacheDir.resolve(filename + ".data");
            if (!Files.exists(dataPath)) {
                try {
                    log.info("Downloading external data file s3://{}/{}", bucket, dataKey);
                    downloadFromS3(dataKey, dataPath);
                } catch (Exception ex) {
                    // External data file is optional - not all models have one
                    log.debug("No external data file found for {}", modelKey);
                }
            }

            return env.createSession(localPath.toString(), new OrtSession.SessionOptions());
        } catch (IOException | OrtException ex) {
            throw new RuntimeException("Failed to load ONNX model: " + modelKey, ex);
        }
    }

    private void downloadFromS3(String key, Path localPath) throws IOException {
        GetObjectRequest req = GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build();
        ResponseBytes<GetObjectResponse> bytes = s3.getObject(req, ResponseTransformer.toBytes());
        Files.write(localPath, bytes.asByteArray());
    }
}
