package com.tahomatracker.service.external;

import java.awt.image.BufferedImage;
import java.io.IOException;
import java.util.Map;

/**
 * Abstraction over object storage operations (e.g., S3).
 */
public interface ObjectStorageClient {
    void putImage(String key, BufferedImage image) throws IOException;
    void putJson(String key, Map<String, Object> payload) throws IOException;
    byte[] getObjectBytes(String key) throws IOException;
    boolean exists(String key) throws IOException;
}
