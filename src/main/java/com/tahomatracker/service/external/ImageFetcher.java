package com.tahomatracker.service.external;

import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * Fetches a single image from a remote camera source.
 *
 * <p>Implementations translate a camera-specific path string into an HTTP
 * request and return the decoded image, or {@code null} when the image is
 * not available (e.g. 404/403).
 */
public interface ImageFetcher {

    /**
     * Fetches the image for the given camera path.
     *
     * @param cameraPath camera-source-specific path string (e.g. Roundshot path "YYYY-MM-DD/HH-mm-00")
     * @return the image, or {@code null} if not available (404/403)
     * @throws IOException          on network or decoding errors
     * @throws InterruptedException if the thread is interrupted during the request
     */
    BufferedImage fetchImage(String cameraPath) throws IOException, InterruptedException;
}
