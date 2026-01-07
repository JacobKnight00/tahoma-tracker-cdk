package com.tahomatracker.service.external;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import javax.imageio.ImageIO;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class S3ObjectStorageClient implements ObjectStorageClient {
    private final S3Client s3;
    private final String bucket;

    public S3ObjectStorageClient(S3Client s3, String bucket) {
        this.s3 = s3;
        this.bucket = bucket;
    }

    @Override
    public void putImage(String key, BufferedImage image) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        if (!ImageIO.write(image, "jpeg", out)) {
            throw new IOException("Failed to encode JPEG");
        }
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("image/jpeg")
                .build();
        s3.putObject(request, RequestBody.fromBytes(out.toByteArray()));
    }

    @Override
    public void putJson(String key, Map<String, Object> payload) throws IOException {
        byte[] bytes = new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsBytes(payload);
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .contentType("application/json")
                .build();
        s3.putObject(request, RequestBody.fromBytes(bytes));
    }

    @Override
    public byte[] getObjectBytes(String key) throws IOException {
        try {
            var req = GetObjectRequest.builder().bucket(bucket).key(key).build();
            var resp = s3.getObjectAsBytes(req);
            return resp.asByteArray();
        } catch (NoSuchKeyException ex) {
            throw new IOException("No such key", ex);
        }
    }

    @Override
    public boolean exists(String key) throws IOException {
        try {
            s3.headObject(HeadObjectRequest.builder().bucket(bucket).key(key).build());
            return true;
        } catch (NoSuchKeyException ex) {
            return false;
        } catch (software.amazon.awssdk.services.s3.model.S3Exception ex) {
            if (ex.statusCode() == 404) {
                return false;
            }
            throw new IOException("Failed to check key: " + key, ex);
        }
    }
}
